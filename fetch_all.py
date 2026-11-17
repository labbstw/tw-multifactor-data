"""多因子回測：FinMind 免費版資料補下載（可中斷、可續傳、開機自動執行）。

- 輸出：多因子/data/fm/{dataset}/{stock_id}.parquet、多因子/data/market/*.parquet、多因子/data/universe.csv
- 只讀不寫 I:/台股888/data/fm（每日流程 p6_fund 會用到它）；其中財報、月營收直接複製過來，省下請求次數。
- 檔案存在 = 已完成，所以任何時候中斷（關機、休眠、網路斷線）下次啟動都會接著跑。
- 自我限速每小時 MAX_PER_HOUR 次，保留額度給每日流程 money888.bat（同一個 FinMind token）。
- 全部完成後跳出提示視窗，並移除開機自動執行的捷徑。

用法：
  pythonw fetch_all.py            # 背景執行（開機捷徑用這個）
  python  fetch_all.py --status   # 看進度
"""
import os, sys, json, time, shutil, logging, collections, re, tarfile
import requests, pandas as pd
try: import ctypes, msvcrt                      # 只有 Windows 有
except ImportError: ctypes = msvcrt = None

HERE = os.path.dirname(os.path.abspath(__file__))
CLOUD = bool(os.environ.get('GITHUB_ACTIONS'))  # 在 GitHub Actions 上跑
DATA = os.environ.get('DATA_DIR') or os.path.join(os.path.dirname(HERE), 'data')
ROOT = os.path.dirname(os.path.dirname(HERE))   # 台股888/（雲端沒有）
API = 'https://api.finmindtrade.com/api/v4/data'
TOKEN = os.environ.get('FINMIND_TOKEN')

def need_token():
    """只有真的要送請求時才取 token（--pack、--status 不需要）；本機從專案 .env 讀。"""
    global TOKEN
    if not TOKEN:
        sys.path.insert(0, os.path.join(ROOT, 'scripts')); import common; TOKEN = common.TOKEN
    return TOKEN
DEADLINE = time.time() + float(os.environ.get('DEADLINE_HOURS', 1e6)) * 3600   # 雲端單次最多跑幾小時

OUT = os.path.join(DATA, 'fm')
MKT = os.path.join(DATA, 'market')
OLD = os.path.join(ROOT, 'data', 'fm')          # 舊研究資料（唯讀，本機才有）
STATUS = os.path.join(DATA, 'download_status.json')
LOG = os.path.join(DATA, 'download.log')
LOCK = os.path.join(DATA, 'download.lock')
MANIFEST = os.path.join(DATA, 'manifest.txt')   # 已完成（含已上傳雲端）的檔案清單，相對於 DATA
STARTUP_LNK = os.path.join(os.environ.get('APPDATA', ''), r'Microsoft\Windows\Start Menu\Programs\Startup',
                           'multifactor_download.lnk')

END = '2026-11-13'            # 評分期間到 2026-10-31，抓到這天即可，所有股票一致
FUND_START = '2015-01-01'     # 財報需 8 季回看 + YoY
DAILY_START = '2017-01-01'    # 暖機 2018 起，再多 250 日
MAX_PER_HOUR = 500            # 免費版 600/小時，留 100 給每日流程
REUSE = {'TaiwanStockMonthRevenue', 'TaiwanStockFinancialStatements', 'TaiwanStockBalanceSheet'}  # 舊資料已涵蓋到評分期末

# 逐檔資料集，依重要性排序：前面的先全部抓完，中途停下也能先用
PER_STOCK = [
    ('TaiwanStockPrice', DAILY_START),
    ('TaiwanStockDividendResult', FUND_START),          # 自行還原股價
    ('TaiwanStockMonthRevenue', FUND_START),
    ('TaiwanStockFinancialStatements', FUND_START),
    ('TaiwanStockBalanceSheet', FUND_START),
    ('TaiwanStockCashFlowsStatement', FUND_START),
    ('TaiwanStockPER', DAILY_START),
    ('TaiwanStockInstitutionalInvestorsBuySell', DAILY_START),
    ('TaiwanStockMarginPurchaseShortSale', DAILY_START),
    ('TaiwanStockShareholding', DAILY_START),
    ('TaiwanStockDividend', FUND_START),
    ('TaiwanStockCapitalReductionReferencePrice', FUND_START),
]
ETFS = ['0050', '0056', '006208', '00713', '00878']
MARKET = [  # (檔名, dataset, params)
    ('TAIEX_price', 'TaiwanStockPrice', {'data_id': 'TAIEX', 'start_date': DAILY_START}),
    ('TPEx_price', 'TaiwanStockPrice', {'data_id': 'TPEx', 'start_date': DAILY_START}),
    ('TAIEX_total_return', 'TaiwanStockTotalReturnIndex', {'data_id': 'TAIEX', 'start_date': DAILY_START}),
    ('TPEx_total_return', 'TaiwanStockTotalReturnIndex', {'data_id': 'TPEx', 'start_date': DAILY_START}),
    ('total_margin', 'TaiwanStockTotalMarginPurchaseShortSale', {'start_date': DAILY_START}),
    ('total_institutional', 'TaiwanStockTotalInstitutionalInvestors', {'start_date': DAILY_START}),
    ('split_price', 'TaiwanStockSplitPrice', {'start_date': FUND_START}),
    ('par_value_change', 'TaiwanStockParValueChange', {'start_date': FUND_START}),
] + [(f'etf_{e}_price', 'TaiwanStockPrice', {'data_id': e, 'start_date': DAILY_START}) for e in ETFS] \
  + [(f'etf_{e}_dividend_result', 'TaiwanStockDividendResult', {'data_id': e, 'start_date': FUND_START}) for e in ETFS]

log = logging.getLogger('dl')


# ---------------------------------------------------------------- 請求
_sent = collections.deque()   # 自己送出的請求時間（滾動一小時）

class OutOfTime(Exception): pass

def wait_for_quota():
    j = requests.get('https://api.web.finmindtrade.com/v2/user_info',
                     headers={'Authorization': f'Bearer {need_token()}'}, timeout=30).json()
    nxt = j.get('next_available_time')               # 台北時間字串（沒有時區），必須明確指定時區
    if nxt: secs = (pd.Timestamp(nxt).tz_localize('Asia/Taipei') - pd.Timestamp.now(tz='UTC')).total_seconds() + 30
    else: secs = 300
    secs = min(max(30, secs), 3700)                  # 一小時視窗，最多等一小時多一點
    log.info(f'限流，等待 {secs / 60:.1f} 分鐘')
    if time.time() + secs > DEADLINE: raise OutOfTime()
    time.sleep(secs)

QUIET = (19 * 60 + 30, 23 * 60)   # 台北時間 19:30–23:00 完全不送請求，讓每晚的 money888.bat 獨享 FinMind 額度

def quiet_wait():
    """在靜默時段內睡到結束；睡不完（超過本次期限）就收工，交給下一次接力。"""
    while True:
        t = time.gmtime(time.time() + 8 * 3600)             # 台北時間（固定 UTC+8，不受本機時區影響）
        m = t.tm_hour * 60 + t.tm_min
        if not (QUIET[0] <= m < QUIET[1]): return
        wake = time.time() + (QUIET[1] - m) * 60 - t.tm_sec + 5
        if wake > DEADLINE: raise OutOfTime()
        time.sleep(min(600, wake - time.time()))

def throttle():
    while True:
        if time.time() > DEADLINE: raise OutOfTime()
        quiet_wait()
        now = time.time()
        while _sent and now - _sent[0] > 3600: _sent.popleft()
        if len(_sent) < MAX_PER_HOUR: break
        time.sleep(min(300, 3600 - (now - _sent[0]) + 1))
    _sent.append(time.time())

class Denied(Exception): pass

def get(dataset, **params):
    """一次請求；網路錯誤（休眠喚醒、斷線）無限重試，限流就等，權限不足丟 Denied。"""
    params.setdefault('end_date', END)
    params = {k: v for k, v in params.items() if v is not None}
    wait = 30
    while True:
        throttle()
        try:
            r = requests.get(API, headers={'Authorization': f'Bearer {need_token()}'},
                             params={'dataset': dataset, **params}, timeout=180)
            if r.status_code == 402:
                log.info('FinMind 限流，等待配額'); wait_for_quota(); continue
            j = r.json()
            if r.status_code == 200: return pd.DataFrame(j.get('data', []))
            msg = str(j.get('msg'))
            if 'level' in msg or r.status_code in (400, 422): raise Denied(f'{r.status_code} {msg[:120]}')
            raise RuntimeError(f'{r.status_code} {msg[:120]}')
        except (Denied, OutOfTime): raise
        except Exception as e:
            log.warning(f'{dataset} {params.get("data_id", "")}: {e}；{wait} 秒後重試')
            time.sleep(wait); wait = min(wait * 2, 900)


_manifest = None
def done(path):
    """檔案已完成：本機有，或清單上有（已上傳雲端、本機沒有）。"""
    global _manifest
    if _manifest is None:
        _manifest = set(open(MANIFEST, encoding='utf-8').read().split(chr(10))) if os.path.exists(MANIFEST) else set()
    return os.path.exists(path) or os.path.relpath(path, DATA).replace(os.sep, '/') in _manifest


# ---------------------------------------------------------------- 股票池
def universe():
    f = os.path.join(DATA, 'universe.csv')
    if os.path.exists(f): return pd.read_csv(f, dtype={'stock_id': str})
    info = get('TaiwanStockInfo', end_date=None)   # 帶日期會被依更新日過濾
    dl = get('TaiwanStockDelisting', start_date='2017-01-01')
    four = r'[1-9]\d{3}'                     # 普通股：4 碼、非 0 開頭（排除 ETF／特別股／權證）
    li = info[info.type.isin(['twse', 'tpex']) & info.stock_id.str.fullmatch(four) & ~info.stock_id.str.startswith('91')]
    li = li.sort_values('date').drop_duplicates('stock_id', keep='last')[['stock_id', 'stock_name', 'type', 'industry_category']]
    dl = dl[dl.stock_id.str.fullmatch(four) & ~dl.stock_id.str.startswith('91')].sort_values('date')
    dl = dl.drop_duplicates('stock_id', keep='last').rename(columns={'date': 'delist_date'})[['stock_id', 'stock_name', 'delist_date']]
    u = li.merge(dl[['stock_id', 'delist_date']], on='stock_id', how='left')
    extra = dl[~dl.stock_id.isin(li.stock_id)].assign(type='', industry_category='')
    u = pd.concat([u, extra], ignore_index=True).sort_values('stock_id')
    os.makedirs(os.path.dirname(f), exist_ok=True)
    u.to_csv(f, index=False, encoding='utf-8-sig')
    info.to_parquet(os.path.join(MKT, 'stock_info_raw.parquet')); dl.to_parquet(os.path.join(MKT, 'delisting.parquet'))
    return u


# ---------------------------------------------------------------- 狀態
def load_status():
    try: return json.load(open(STATUS, encoding='utf-8'))
    except Exception: return {'denied': {}, 'failed': {}}

def save_status(st):
    st['updated'] = time.strftime('%Y-%m-%d %H:%M')
    tmp = STATUS + '.tmp'
    json.dump(st, open(tmp, 'w', encoding='utf-8'), ensure_ascii=False, indent=1); os.replace(tmp, STATUS)

def progress(u):
    rows, n_all, tot_all = [], 0, 0
    for ds, _ in PER_STOCK:
        d = os.path.join(OUT, ds)
        n = sum(1 for sid in u.stock_id if done(os.path.join(d, sid + '.parquet'))); rows.append((ds, n, len(u)))
        n_all += n; tot_all += len(u)
    return rows, n_all, tot_all


def notify(title, text):
    if CLOUD or ctypes is None: return
    try: ctypes.windll.user32.MessageBoxW(0, text, title, 0x40 | 0x10000 | 0x40000)
    except Exception: pass


def pack(tag):
    """雲端：把這次新抓的檔案（不在清單上的）打成 chunk-{tag}.tar，並更新 manifest.txt。"""
    done(MANIFEST)   # 載入清單
    new = []
    for base, _, files in os.walk(DATA):
        for f in files:
            rel = os.path.relpath(os.path.join(base, f), DATA).replace(os.sep, '/')
            if f.endswith(('.parquet', '.csv')) and rel != 'universe.csv' and rel not in _manifest: new.append(rel)
    out = os.path.join(DATA, f'chunk-{tag}.tar')
    if new:
        with tarfile.open(out, 'w') as t:
            for rel in sorted(new): t.add(os.path.join(DATA, rel), arcname=rel)
        with open(MANIFEST, 'a', encoding='utf-8') as m:
            for rel in sorted(new): m.write(rel + chr(10))
    print(f'pack: {len(new)} 個新檔案 -> {out if new else "(無)"}')


# ---------------------------------------------------------------- 主流程
def run():
    os.makedirs(OUT, exist_ok=True); os.makedirs(MKT, exist_ok=True)
    st = load_status()
    log.info('==== 啟動 ====')
    for name, ds, p in MARKET:
        f = os.path.join(MKT, name + '.parquet')
        if done(f) or name in st['denied']: continue
        try: get(ds, **p).to_parquet(f); log.info(f'市場資料 {name} 完成')
        except Denied as e: st['denied'][name] = str(e); save_status(st); log.warning(f'{name} 無權限：{e}')
    u = universe()
    log.info(f'股票池 {len(u)} 檔')
    for ds, start in PER_STOCK:
        if ds in st['denied']: continue
        d = os.path.join(OUT, ds); os.makedirs(d, exist_ok=True)
        todo = [s for s in u.stock_id if not done(os.path.join(d, s + '.parquet'))]
        if not todo: continue
        log.info(f'{ds}：剩 {len(todo)} 檔')
        for i, sid in enumerate(todo):
            dst = os.path.join(d, sid + '.parquet')
            src = os.path.join(OLD, ds, sid + '.parquet')
            if ds in REUSE and os.path.exists(src):   # 本機才有舊資料
                shutil.copy2(src, dst); continue
            try:
                get(ds, data_id=sid, start_date=start).to_parquet(dst + '.tmp'); os.replace(dst + '.tmp', dst)
            except Denied as e:
                key = f'{ds}/{sid}'
                st['failed'][key] = st['failed'].get(key, 0) + 1
                if i < 3 and sum(1 for k in st['failed'] if k.startswith(ds + '/')) >= 3:   # 整個資料集都沒權限
                    st['denied'][ds] = str(e); save_status(st); log.warning(f'{ds} 無權限，略過：{e}'); break
                if st['failed'][key] >= 3: pd.DataFrame().to_parquet(dst)                   # 單檔重試 3 次就放棄
                save_status(st)
            if i % 100 == 0:
                save_status(st); log.info(f'{ds} {i}/{len(todo)}')
    rows, n_done, tot = progress(u)
    denied = list(st['denied'])
    st['finished'] = n_done >= tot - sum(len(u) for ds, _ in PER_STOCK if ds in st['denied'])
    save_status(st)
    if st['finished']:
        log.info('==== 全部完成 ====')
        try: os.remove(STARTUP_LNK)
        except (FileNotFoundError, OSError): pass
        if CLOUD: open(os.path.join(DATA, 'finished.flag'), 'w').close()
        msg = (f'多因子回測資料下載完成！\n\n股票池 {len(u)} 檔，存在：\n{OUT}\n\n'
               + (f'無權限略過：{", ".join(denied)}\n\n' if denied else '')
               + '開機自動執行已移除。回到 Claude Code 說「開始批次 0」即可。')
        notify('台股多因子：資料下載完成', msg)


def main():
    os.makedirs(DATA, exist_ok=True); os.makedirs(MKT, exist_ok=True)
    if '--status' in sys.argv:
        u = pd.read_csv(os.path.join(DATA, 'universe.csv'), dtype={'stock_id': str})
        rows, done, tot = progress(u)
        for ds, n, t in rows: print(f'{ds:45s} {n:5d}/{t}')
        st = load_status()
        print(f'\n合計 {done}/{tot}（{done / tot:.1%}），更新於 {st.get("updated")}，完成={st.get("finished", False)}')
        print('無權限：', st.get('denied'))
        return
    if '--pack' in sys.argv: pack(sys.argv[sys.argv.index('--pack') + 1]); return
    logging.basicConfig(filename=None if CLOUD else LOG, level=logging.INFO, encoding='utf-8',
                        format='%(asctime)s %(levelname)s %(message)s')
    if msvcrt:                                   # 本機：避免同時跑兩份（雲端靠 concurrency 保證）
        lock = open(LOCK, 'w')
        try: msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError: return
    try: run()
    except OutOfTime: log.info('單次時間用完，存檔後由下一次接力')
    except Exception as e:
        log.exception('未預期錯誤')
        notify('台股多因子：下載程式出錯', f'{e}\n\n下次開機會自動重試。詳細見：\n{LOG}')
        if CLOUD: sys.exit(1)                     # 雲端：讓這次執行顯示失敗，不要接力空轉


if __name__ == '__main__':
    main()

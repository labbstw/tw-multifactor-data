"""把雲端 Release "data" 的下載成果拉回本機 多因子/data/（已存在的檔案不覆蓋）。

用法：python pull_from_cloud.py <owner/repo>
需要已登入的 gh。可重複執行；只下載尚未處理過的 chunk。
"""
import os, sys, subprocess, tarfile, glob, tempfile

repo = sys.argv[1]
DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(DATA, exist_ok=True)
seen_f = os.path.join(DATA, 'pulled_chunks.txt')
seen = set(open(seen_f).read().split()) if os.path.exists(seen_f) else set()

names = subprocess.run(['gh', 'release', 'view', 'data', '-R', repo, '--json', 'assets', '-q', '.assets[].name'],
                       capture_output=True, text=True, check=True).stdout.split()
chunks = sorted(n for n in names if n.startswith('chunk-') and n not in seen)
print(f'雲端共 {sum(n.startswith("chunk-") for n in names)} 個 chunk，待拉回 {len(chunks)} 個')
with tempfile.TemporaryDirectory() as tmp:
    for n in chunks:
        subprocess.run(['gh', 'release', 'download', 'data', '-R', repo, '-p', n, '-D', tmp, '--clobber'], check=True)
        added = 0
        with tarfile.open(os.path.join(tmp, n)) as t:
            for m in t.getmembers():
                if not m.isfile() or '..' in m.name or os.path.isabs(m.name): continue
                dst = os.path.join(DATA, m.name)
                if os.path.exists(dst): continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with open(dst, 'wb') as f: f.write(t.extractfile(m).read())
                added += 1
        os.remove(os.path.join(tmp, n)); seen.add(n)
        open(seen_f, 'w').write('\n'.join(sorted(seen)))
        print(f'{n}: 新增 {added} 檔')
for f in ('universe.csv', 'download_status.json'):
    subprocess.run(['gh', 'release', 'download', 'data', '-R', repo, '-p', f, '-D', DATA, '--clobber'])
print('完成。進度：python fetch_all.py --status')

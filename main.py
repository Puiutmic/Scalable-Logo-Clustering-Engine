import pandas as pd
import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PIL import Image
import imagehash
import io
import json

class LogoClustered:
    def __init__(self):
        self.parent = {}
    def find(self, i):
        if self.parent[i] == i: return i
        self.parent[i] = self.find(self.parent[i])
        return self.parent[i]
    def union(self, i, j):
        root_i, root_j = self.find(i), self.find(j)
        if root_i != root_j: self.parent[root_i] = root_j

async def get_logo_url(session, domain):
    url = f"https://{domain}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }
    try:
        async with session.get(url, timeout=10, headers=headers, allow_redirects=True) as response:
            if response.status != 200: 
                return None
            
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            meta = soup.find('meta', property='og:image')
            if meta and meta.get('content'): return urljoin(url, meta['content'])
            
            icon = soup.find('link', rel=lambda x: x and any(s in x.lower() for s in ['icon', 'logo']))
            if icon and icon.get('href'): return urljoin(url, icon['href'])
            
            for img in soup.find_all('img'):
                src = img.get('src', '')
                alt = img.get('alt', '').lower()
                cls = str(img.get('class', '')).lower()
                if any(k in src.lower() or k in alt or k in cls for k in ['logo', 'brand', 'nav-img']):
                    return urljoin(url, src)
    except:
        pass
    return None

async def main():
    print("Reading dataset...")
    df = pd.read_parquet("logos.snappy.parquet", engine='pyarrow')
    domains = df['domain'].dropna().unique().tolist()
    
    limit = 300 
    subset = domains[:limit]
    hashes = {}
    
    print(f"Engine started. Processing {len(subset)} domains...\n")
    
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [get_logo_url(session, d) for d in subset]
        logo_urls = await asyncio.gather(*tasks)
        
        for i, url in enumerate(logo_urls):
            if not url or any(x in url.lower() for x in ['.svg', '.php', '.gif']): 
                continue
            try:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        img_bytes = await resp.read()
                        img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
                        hashes[subset[i]] = imagehash.phash(img)
                        print(f"[SUCCESS] Hashed: {subset[i]}")
            except:
                continue

    success_rate = (len(hashes) / len(subset)) * 100
    print(f"\n--- PERFORMANCE METRICS ---")
    print(f"Success Rate: {success_rate:.2f}%")
    print(f"Logos Hashed: {len(hashes)}")

    print("\nRunning DSU Clustering...")
    dsu = LogoClustered()
    for d in hashes: dsu.parent[d] = d
    items = list(hashes.keys())
    
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            if hashes[items[i]] - hashes[items[j]] <= 8:
                dsu.union(items[i], items[j])

    groups = {}
    for d in items:
        root = dsu.find(d)
        if root not in groups: groups[root] = []
        groups[root].append(d)

    with open("results.json", "w") as f:
        json.dump(list(groups.values()), f, indent=4)
    print("Done! Check results.json")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
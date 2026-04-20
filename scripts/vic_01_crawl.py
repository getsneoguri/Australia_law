import hashlib, json, re, random, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.legislation.vic.gov.au"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AustraliaLawMCP/1.0)", "Accept-Language": "en-AU,en;q=0.9"}
JURISDICTION, SOURCE, MIME, SNIPPET_CHARS = "vic", "legislation.vic.gov.au", "text/html", 2000
DELAY_MIN, DELAY_MAX, BATCH_SIZE = 1.0, 2.5, 50
_SCRIPT_DIR = Path(__file__).parent
DATA_DIR = _SCRIPT_DIR.parent / "data"
OUT_PATH = DATA_DIR / "vic_documents.parquet"
CHECKPOINT_PATH = DATA_DIR / "vic_crawl_checkpoint.json"
ERROR_LOG_PATH = DATA_DIR / "vic_crawl_errors.log"
SEED_PATH = DATA_DIR / "vic_seed_urls.json"
SCHEMA = pa.schema([("version_id", pa.string()), ("title", pa.string()), ("jurisdiction", pa.string()), ("doc_type", pa.string()), ("date", pa.string()), ("citation", pa.string()), ("url", pa.string()), ("source", pa.string()), ("mime", pa.string()), ("when_scraped", pa.string()), ("text_snippet", pa.string())])

def make_version_id(url): return f"vic/{hashlib.sha256(url.encode()).hexdigest()[:16]}"
def load_checkpoint():
    if CHECKPOINT_PATH.exists():
        try: return set(json.loads(CHECKPOINT_PATH.read_text()).get("done", []))
        except: return set()
    return set()
def save_checkpoint(done): CHECKPOINT_PATH.write_text(json.dumps({"done": sorted(done)}, indent=2))
def log_error(url, msg):
    with ERROR_LOG_PATH.open("a") as f: f.write(f"{datetime.now(timezone.utc).isoformat()}\t{url}\t{msg}\n")
def fetch_html(url, session, retries=3):
    for attempt in range(1, retries+1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30); resp.raise_for_status(); return resp.text
        except requests.RequestException:
            if attempt == retries: return None
            time.sleep(2**attempt)
def open_writer(path): return pq.ParquetWriter(str(path), SCHEMA, compression="zstd")
def write_batch(writer, batch):
    cols = {k: [] for k in SCHEMA.names}
    for rec in batch:
        for k in SCHEMA.names: cols[k].append(rec.get(k))
    writer.write_table(pa.table(cols, schema=SCHEMA))

def parse_page(base_url, doc_type, html):
    soup = BeautifulSoup(html, "html.parser")
    now_utc = datetime.now(timezone.utc).isoformat()
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else base_url.rstrip("/").split("/")[-1].replace("-"," ").title()
    full_text = soup.get_text(separator="\n")
    citation = None
    for pat in [r"Act\s+(?:number|no\.?)\s+([\d/]+)", r"S\.R\.\s*No\.?\s*([\d/]+)"]:
        m = re.search(pat, full_text, re.IGNORECASE)
        if m:
            num = m.group(1).strip()
            citation = f"Act No. {num}" if doc_type == "act" else f"S.R. No. {num}"; break
    latest_url, latest_ver = base_url, -1
    for a in soup.find_all("a", href=True):
        full = urljoin(BASE_URL, a["href"]); parts = [p for p in urlparse(full).path.split("/") if p]
        if len(parts) >= 4 and parts[0] == "in-force" and parts[-1].isdigit():
            ver_num = int(parts[-1])
            if ver_num > latest_ver: latest_ver = ver_num; latest_url = full
    latest_date = None
    for pat in [r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})", r"(\d{4}-\d{2}-\d{2})"]:
        m = re.search(pat, full_text, re.IGNORECASE)
        if m:
            raw = m.group(1)
            try:
                if re.match(r"\d{1,2}\s+\w+\s+\d{4}", raw): latest_date = datetime.strptime(raw, "%d %b %Y").strftime("%Y-%m-%d")
                else: latest_date = raw
            except: latest_date = raw
            break
    content = ""
    for sel in ["main", "[role='main']", "#content", "article"]:
        el = soup.select_one(sel)
        if el:
            for rm in el.find_all(["nav","header","footer","script","style"]): rm.decompose()
            content = el.get_text(separator=" ", strip=True); break
    if not content:
        body = soup.find("body")
        if body:
            for rm in body.find_all(["nav","header","footer","script","style"]): rm.decompose()
            content = body.get_text(separator=" ", strip=True)
    return {"version_id": make_version_id(latest_url), "title": title, "jurisdiction": JURISDICTION, "doc_type": doc_type, "date": latest_date, "citation": citation, "url": latest_url, "source": SOURCE, "mime": MIME, "when_scraped": now_utc, "text_snippet": content[:SNIPPET_CHARS].strip() if content else None}

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    done_urls = load_checkpoint()
    print(f"[VIC-01] 크롤러 시작 | 체크포인트: {len(done_urls)}개 완료")
    seed_items = json.loads(SEED_PATH.read_text())
    print(f"[VIC-01] seed {len(seed_items)}개 로드")
    total = len(seed_items); writer = None; rows_written = 0; errors = 0; batch = []
    print(f"[VIC-01] 크롤링 시작 ({total}개)...")
    for idx, item in enumerate(seed_items, 1):
        url = item["url"]; doc_type = item.get("doc_type", "act")
        if url in done_urls: continue
        html = fetch_html(url, session)
        if not html: log_error(url, "fetch_failed"); errors += 1; done_urls.add(url); continue
        if "Page not found" in html or len(html) < 500: log_error(url, "404"); done_urls.add(url); continue
        record = parse_page(url, doc_type, html)
        if record: batch.append(record); rows_written += 1
        else: log_error(url, "parse_failed"); errors += 1
        done_urls.add(url)
        if idx % 10 == 0 or idx == total:
            print(f"[VIC-01] {idx}/{total} ({idx/total*100:.1f}%) | 저장: {rows_written} | 오류: {errors}", flush=True)
        if len(batch) >= BATCH_SIZE:
            if writer is None: writer = open_writer(OUT_PATH)
            write_batch(writer, batch); batch.clear(); save_checkpoint(done_urls)
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    if batch:
        if writer is None: writer = open_writer(OUT_PATH)
        write_batch(writer, batch); save_checkpoint(done_urls)
    if writer: writer.close()
    size_mb = OUT_PATH.stat().st_size/1_048_576 if OUT_PATH.exists() else 0
    print(f"\n[VIC-01] 완료. {rows_written}개 → {OUT_PATH} ({size_mb:.1f}MB) | 오류: {errors}")
    return 0

if __name__ == "__main__": sys.exit(main())

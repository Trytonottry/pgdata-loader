#!/usr/bin/env python3
# etl.py — основной ingestion engine (light version, optimized for HP DL380p Gen8)
import os
import csv
import gzip
import json
import yaml
import orjson
import logging
import argparse
import tempfile
import shutil
import hashlib
import time
from pathlib import Path
from datetime import datetime
from multiprocessing import Process, Queue, Event, cpu_count
from typing import Dict, Any
import psycopg2
import psycopg2.extras
import pandas as pd
from prometheus_client import Counter, Gauge
from utils.logging import setup_logging

# Metrics (Prometheus)
FILES_PROCESSED = Counter('etl_files_processed_total', 'Files processed')
ROWS_IMPORTED = Counter('etl_rows_imported_total', 'Rows imported')
FILES_FAILED = Counter('etl_files_failed_total', 'Files failed')

# ---------------- utils ----------------
def load_config(path: str):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def compute_sha256(path: Path, block=65536):
    h = hashlib.sha256()
    with path.open('rb') as fd:
        for b in iter(lambda: fd.read(block), b''):
            h.update(b)
    return h.hexdigest()

def pg_conn_from_sqlalchemy_url(pg_url: str):
    # Accept sqlalchemy-like url (postgresql+psycopg2://...)
    if pg_url.startswith("postgresql+psycopg2://"):
        dsn = pg_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    else:
        dsn = pg_url
    return psycopg2.connect(dsn)

# ---------------- Parsers (streaming) ----------------
def iter_csv(path: Path, chunksize=10000):
    for chunk in pd.read_csv(path, chunksize=chunksize, dtype=str, keep_default_na=False, low_memory=False, encoding='utf-8'):
        for idx, row in chunk.iterrows():
            yield row.to_dict()

def iter_json_or_jsonl(path: Path):
    with path.open('r', encoding='utf-8', errors='replace') as f:
        head = f.read(2048).lstrip()
        f.seek(0)
        if head.startswith('['):
            arr = json.load(f)
            for item in arr:
                yield item
        else:
            for line in f:
                line=line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except:
                    yield {'line': line}

def iter_gz(path: Path):
    with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except:
                yield {'line': line}

def iter_xlsx(path: Path):
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        try:
            for chunk in pd.read_excel(xls, sheet_name=sheet, chunksize=10000, dtype=str, engine='openpyxl', keep_default_na=False):
                for _, row in chunk.iterrows():
                    yield row.to_dict()
        except ValueError:
            df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl', dtype=str, keep_default_na=False)
            for _, row in df.iterrows():
                yield row.to_dict()

def iter_parquet(path: Path):
    # pandas reading; may require pyarrow or fastparquet
    try:
        for chunk in pd.read_parquet(path, chunksize=10000):
            for _, row in chunk.iterrows():
                yield row.to_dict()
    except Exception:
        df = pd.read_parquet(path)
        for _, row in df.iterrows():
            yield row.to_dict()

def iter_sqlite(path: Path):
    import sqlite3
    con = sqlite3.connect(str(path))
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        for chunk in pd.read_sql_query(f'SELECT * FROM "{t}"', con, chunksize=10000):
            for _, row in chunk.iterrows():
                yield row.to_dict()
    con.close()

# dispatch
def detect_iter(path: Path):
    s = path.suffix.lower()
    if s == '.csv':
        return iter_csv(path)
    if s in ('.json', '.jsonl'):
        return iter_json_or_jsonl(path)
    if s == '.gz':
        return iter_gz(path)
    if s in ('.xls', '.xlsx'):
        return iter_xlsx(path)
    if s == '.parquet':
        return iter_parquet(path)
    if s in ('.db', '.sqlite'):
        return iter_sqlite(path)
    # fallback: plain text lines
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for i, line in enumerate(f):
            yield {'line': line.rstrip('\n\r')}

# ---------------- Worker process ----------------
def worker_proc(task_q: Queue, meta_q: Queue, tmpdir: str, cfg: Dict[str,Any], stop_event: Event):
    logger = logging.getLogger("etl.worker")
    logger.info("worker started")
    while not stop_event.is_set():
        try:
            path = task_q.get(timeout=1)
        except Exception:
            continue
        if path == "STOP":
            break
        p = Path(path)
        try:
            sha = compute_sha256(p)
            rows = 0
            tfile = Path(tmpdir) / f"tmp_{os.getpid()}_{int(time.time()*1000)}.tsv"
            tfile.parent.mkdir(parents=True, exist_ok=True)
            with tfile.open('w', encoding='utf-8', newline='') as fh:
                writer = csv.writer(fh, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                for rec in detect_iter(p):
                    try:
                        blob = orjson.dumps(rec).decode('utf-8')
                    except Exception:
                        blob = json.dumps(rec, ensure_ascii=False)
                    writer.writerow([str(p), blob])
                    rows += 1
                    if rows % 20000 == 0:
                        fh.flush()
            meta_q.put({'tmpfile': str(tfile), 'rows': rows, 'source_file': str(p), 'sha256': sha, 'mtime': p.stat().st_mtime, 'size': p.stat().st_size})
            logger.info(f"worker wrote {rows} rows for {p}")
        except Exception as e:
            # log error and continue
            logger.exception(f"worker failed for {p}: {e}")
            meta_q.put({'error': str(e), 'file': str(p)})
    logger.info("worker exiting")

# ---------------- Writer process ----------------
def writer_proc(meta_q: Queue, cfg: Dict[str,Any], stop_event: Event):
    logger = logging.getLogger("etl.writer")
    tmpdir = cfg['tmp_dir']
    conn = None
    try:
        conn = pg_conn_from_sqlalchemy_url(cfg['pg_url'])
        cur = conn.cursor()
        # ensure table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_rows (
                id BIGSERIAL PRIMARY KEY,
                source_file TEXT,
                data JSONB,
                imported_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        conn.commit()
    except Exception as e:
        logger.exception("writer DB init failed")
        raise

    while not stop_event.is_set() or not meta_q.empty():
        try:
            meta = meta_q.get(timeout=1)
        except Exception:
            continue
        if meta == "STOP":
            break
        if 'error' in meta:
            logger.error(f"Worker error: {meta.get('error')} file={meta.get('file')}")
            FILES_FAILED.inc()
            continue
        tmpfile = meta.get('tmpfile')
        rows = meta.get('rows', 0)
        source_file = meta.get('source_file')
        try:
            # Use COPY
            with open(tmpfile, 'r', encoding='utf-8') as f:
                cur.copy_expert("COPY raw_rows (source_file, data) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"')", f)
                conn.commit()
            ROWS_IMPORTED.inc(rows)
            FILES_PROCESSED.inc()
            logger.info(f"Imported {rows} rows from {source_file}")
            # remove tmp
            try:
                os.remove(tmpfile)
            except:
                pass
        except Exception as e:
            conn.rollback()
            logger.exception(f"writer failed import tmpfile {tmpfile}: {e}")
            FILES_FAILED.inc()
            # move tmpfile to errors area for manual inspection
            err_dir = Path(cfg['tmp_dir']) / "failed"
            err_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(tmpfile, err_dir / Path(tmpfile).name)
            # write error log
            with open(cfg['error_log'], 'a', encoding='utf-8') as ef:
                ef.write(json.dumps({'ts': datetime.utcnow().isoformat()+'Z','file': source_file, 'error': str(e)}) + "\n")

    try:
        cur.close()
        conn.close()
    except:
        pass
    logger.info("writer exiting")

# ---------------- Coordinator & main ----------------
def find_files(root: str, include_exts=None):
    for p in Path(root).rglob('*'):
        if p.is_file():
            if include_exts:
                if p.suffix.lower() in include_exts:
                    yield str(p)
            else:
                yield str(p)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='/opt/etl_loader/config.yaml')
    args = parser.parse_args()
    cfg = load_config(args.config)

    # logging
    setup_logging(cfg['log_file'], cfg['error_log'])

    logger = logging.getLogger("etl.main")
    logger.info("ETL starting")
    tmpdir = cfg.get('tmp_dir') or tempfile.mkdtemp(prefix='etl_tmp_')
    Path(tmpdir).mkdir(parents=True, exist_ok=True)

    task_q = Queue()
    meta_q = Queue()
    stop_event = Event()

    # spawn writer
    writer = Process(target=writer_proc, args=(meta_q, cfg, stop_event), daemon=True)
    writer.start()

    # spawn workers
    workers = []
    workers_n = cfg.get('workers', max(2, cpu_count()-1))
    for i in range(workers_n):
        p = Process(target=worker_proc, args=(task_q, meta_q, tmpdir, cfg, stop_event), daemon=True)
        p.start()
        workers.append(p)

    # enqueue files (optionally compute hash to skip existing — currently not skipping)
    files = list(find_files(cfg['data_dir']))
    logger.info(f"Found {len(files)} files")
    for f in files:
        task_q.put(f)

    # send STOP to workers
    for _ in workers:
        task_q.put("STOP")

    # wait
    for w in workers:
        w.join()

    # signal writer then wait
    meta_q.put("STOP")
    writer.join()

    logger.info("ETL finished")

if __name__ == "__main__":
    main()

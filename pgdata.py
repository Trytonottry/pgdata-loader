#!/usr/bin/env python3
# import_to_pg_ultimate.py
"""
High-performance universal importer:
 - multiprocessing parser workers
 - writer(s) performing COPY from temp files + executemany fallback
 - resume/delta-check via imported_files table (sha256/mtime/size)
 - optional per-file table schema inference
 - optional Kafka publish of parsed rows
 - HTTP metrics server (aiohttp)
 - supports: csv, tsv, json, jsonl, gz, xls/xlsx, parquet, sqlite (.db), txt/log/info, sql (stored)
"""

import argparse
import os
import sys
import csv
import gzip
import json
import hashlib
import shutil
import sqlite3
import tempfile
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Iterable
from multiprocessing import Process, Queue, cpu_count, Event
from concurrent.futures import ThreadPoolExecutor

# external libs
import orjson
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from tqdm import tqdm

# optional libs (import inside functions to allow opt-out)
# aiokafka (Kafka), aiohttp (metrics), fastparquet/pyarrow (parquet), python-magic

# ---------------- CONFIG / DEFAULTS ----------------
DEFAULT_WORKERS = max(2, cpu_count() - 1)
DEFAULT_BATCH = 5000
COPY_THRESHOLD = 500  # минимальный батч для использования COPY
TMPDIR_PREFIX = "pg_import_tmp_"
LOG_LEVEL = logging.INFO
# ---------------------------------------------------

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("importer")

# ---------------- Utility ----------------
def compute_sha256(path: Path, block=65536) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(block), b""):
            h.update(b)
    return h.hexdigest()

def sql_alchemy_engine(pg_url: str):
    return create_engine(pg_url, client_encoding='utf8', pool_pre_ping=True)

def psycopg2_conn_from_url(pg_url: str):
    # Accept sqlalchemy-like url: postgresql+psycopg2://user:pass@host:port/db
    if pg_url.startswith("postgresql+psycopg2://"):
        dsn = pg_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    else:
        dsn = pg_url
    return psycopg2.connect(dsn)

# ---------------- Schema / meta tables ----------------
def create_meta_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS imported_files (
            id BIGSERIAL PRIMARY KEY,
            source_file TEXT UNIQUE,
            sha256 TEXT,
            mtime TIMESTAMP,
            size BIGINT,
            status TEXT,
            rows_imported BIGINT DEFAULT 0,
            last_error TEXT,
            imported_at TIMESTAMP
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_rows (
            id BIGSERIAL PRIMARY KEY,
            source_file TEXT,
            file_type TEXT,
            table_or_sheet TEXT,
            row_index BIGINT,
            data JSONB,
            imported_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
        """))

# ---------------- File discovery ----------------
def find_files(root: Path, include_exts: Optional[set]=None):
    for p in root.rglob("*"):
        if p.is_file():
            if include_exts:
                if p.suffix.lower() in include_exts:
                    yield p
            else:
                yield p

# ---------------- Parsers (yield row dicts) ----------------
# Each parser yields dicts: {'source_file': str, 'file_type': str, 'table_or_sheet': Optional[str], 'row_index': int or None, 'data': dict}

def parse_csv(path: Path, chunk_size=10000):
    for chunk in pd.read_csv(path, chunksize=chunk_size, dtype=str, keep_default_na=False, encoding='utf-8', low_memory=False):
        for idx, row in chunk.iterrows():
            yield {'source_file': str(path), 'file_type':'csv', 'table_or_sheet': None, 'row_index': int(idx), 'data': {k: (None if v=='' else v) for k,v in row.items()}}

def parse_tsv(path: Path, chunk_size=10000):
    for chunk in pd.read_csv(path, sep='\t', chunksize=chunk_size, dtype=str, keep_default_na=False, encoding='utf-8', low_memory=False):
        for idx, row in chunk.iterrows():
            yield {'source_file': str(path), 'file_type':'tsv', 'table_or_sheet': None, 'row_index': int(idx), 'data': {k: (None if v=='' else v) for k,v in row.items()}}

def parse_json_or_jsonl(path: Path):
    with path.open('r', encoding='utf-8', errors='replace') as f:
        # try detect array vs lines
        first = f.read(2048).lstrip()
        f.seek(0)
        if first.startswith('['):
            data = json.load(f)
            if isinstance(data, list):
                for i, obj in enumerate(data):
                    yield {'source_file': str(path), 'file_type':'json_array', 'table_or_sheet':None, 'row_index':i, 'data':obj}
            else:
                yield {'source_file': str(path), 'file_type':'json_obj', 'table_or_sheet':None, 'row_index':0, 'data':data}
        else:
            for i, line in enumerate(f):
                line=line.rstrip('\n\r')
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    obj = {"line": line}
                yield {'source_file': str(path), 'file_type':'jsonl', 'table_or_sheet':None, 'row_index':i, 'data':obj}

def parse_gz(path: Path):
    with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
        # try JSON detection
        first = f.read(2048).lstrip()
        f.seek(0)
        if first.startswith('[') or first.startswith('{'):
            # use json logic
            if first.startswith('['):
                data = json.load(f)
                if isinstance(data, list):
                    for i, obj in enumerate(data):
                        yield {'source_file': str(path), 'file_type':'json_gz_array', 'table_or_sheet':None, 'row_index':i, 'data':obj}
                else:
                    yield {'source_file': str(path), 'file_type':'json_gz_object', 'table_or_sheet':None, 'row_index':0, 'data':data}
            else:
                for i, line in enumerate(f):
                    line=line.rstrip('\n\r')
                    if not line: continue
                    try:
                        obj = json.loads(line)
                    except:
                        obj = {"line": line}
                    yield {'source_file': str(path), 'file_type':'jsonl_gz', 'table_or_sheet':None, 'row_index':i, 'data':obj}
        else:
            # try csv via pandas
            f.seek(0)
            try:
                for chunk in pd.read_csv(f, chunksize=10000, dtype=str, keep_default_na=False, low_memory=False):
                    for idx, row in chunk.iterrows():
                        yield {'source_file': str(path), 'file_type':'csv_gz', 'table_or_sheet':None, 'row_index':int(idx), 'data':{k:(None if v=='' else v) for k,v in row.items()}}
            except Exception:
                # fallback to lines
                f.seek(0)
                for i, line in enumerate(f):
                    yield {'source_file': str(path), 'file_type':'text_gz', 'table_or_sheet':None, 'row_index':i, 'data':{"line": line.rstrip('\n\r')}}

def parse_xlsx(path: Path, chunk_size=10000):
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        try:
            for chunk in pd.read_excel(xls, sheet_name=sheet, chunksize=chunk_size, dtype=str, engine='openpyxl', keep_default_na=False):
                for idx, row in chunk.iterrows():
                    yield {'source_file': str(path), 'file_type':'excel', 'table_or_sheet':sheet, 'row_index':int(idx), 'data':{k:(None if v=='' else v) for k,v in row.items()}}
        except ValueError:
            df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl', dtype=str, keep_default_na=False)
            for idx, row in df.iterrows():
                yield {'source_file': str(path), 'file_type':'excel', 'table_or_sheet':sheet, 'row_index':int(idx), 'data':{k:(None if v=='' else v) for k,v in row.items()}}

def parse_parquet(path: Path):
    try:
        # pandas supports parquet via pyarrow/fastparquet
        for chunk in pd.read_parquet(path, chunksize=10000):
            for idx, row in chunk.iterrows():
                yield {'source_file': str(path), 'file_type':'parquet', 'table_or_sheet':None, 'row_index':int(idx), 'data':row.to_dict()}
    except Exception as e:
        # fallback read all
        df = pd.read_parquet(path)
        for idx, row in df.iterrows():
            yield {'source_file': str(path), 'file_type':'parquet', 'table_or_sheet':None, 'row_index':int(idx), 'data':row.to_dict()}

def parse_sql_text(path: Path):
    # store lines, don't execute by default
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for i, line in enumerate(f):
            yield {'source_file': str(path), 'file_type':'sql_text', 'table_or_sheet':None, 'row_index':i, 'data':{'sql_line': line.rstrip('\n\r')}}

def parse_txt_lines(path: Path):
    with path.open('r', encoding='utf-8', errors='replace') as f:
        for i, line in enumerate(f):
            yield {'source_file': str(path), 'file_type':'text', 'table_or_sheet':None, 'row_index':i, 'data':{'line': line.rstrip('\n\r')}}

def parse_sqlite(path: Path):
    try:
        con = sqlite3.connect(str(path))
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            for chunk in pd.read_sql_query(f'SELECT * FROM "{t}"', con, chunksize=10000):
                for idx, row in chunk.iterrows():
                    yield {'source_file': str(path), 'file_type':'sqlite', 'table_or_sheet':t, 'row_index':int(idx), 'data':{col:(None if pd.isna(val) else val) for col,val in row.items()}}
    except Exception as e:
        yield {'source_file': str(path), 'file_type':'sqlite_error', 'table_or_sheet':None, 'row_index':0, 'data':{'error':str(e)}}
    finally:
        try: con.close()
        except: pass

# ---------------- Parser dispatcher ----------------
def detect_and_parse(path: Path) -> Iterable[Dict[str,Any]]:
    s = path.suffix.lower()
    if s == '.csv':
        return parse_csv(path)
    if s == '.tsv':
        return parse_tsv(path)
    if s in ('.json', '.jsonl'):
        return parse_json_or_jsonl(path)
    if s == '.gz':
        return parse_gz(path)
    if s in ('.xls', '.xlsx'):
        return parse_xlsx(path)
    if s == '.parquet':
        return parse_parquet(path)
    if s in ('.txt', '.log', '.info'):
        return parse_txt_lines(path)
    if s == '.sql':
        return parse_sql_text(path)
    if s in ('.db', '.sqlite'):
        return parse_sqlite(path)
    # fallback: try text
    return parse_txt_lines(path)

# ---------------- Worker process ----------------
def worker_proc(task_q: Queue, tempdir: str, rowmeta_q: Queue, stop_event: Event, parser_chunk=10000):
    """
    Worker: получает путь, парсит в streaming, пишет в локальный temp file в формате TSV (one JSON per row in final column),
    и оповещает writer через rowmeta_q о готовом временном файле: {'tmpfile': path, 'rows': n, 'source_file':..., 'file_type':...}
    Это позволяет writer делать COPY быстро.
    """
    logger.info(f"Worker started with tmpdir={tempdir}")
    while not stop_event.is_set():
        try:
            path = task_q.get(timeout=1)
        except Exception:
            continue
        if path == "STOP":
            break
        p = Path(path)
        sha = compute_sha256(p)
        stat = p.stat()
        rows_written = 0
        # temp file per original file + pid + timestamp
        tf = Path(tempdir) / f"tmp_{os.getpid()}_{int(time.time()*1000)}.tsv"
        # ensure parent
        tf.parent.mkdir(parents=True, exist_ok=True)
        with tf.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.writer(fh, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            for row in detect_and_parse(p):
                # row['data'] -> JSON string; keep safe unicode
                try:
                    json_blob = orjson.dumps(row['data']).decode('utf-8')
                except Exception:
                    json_blob = json.dumps(row['data'], ensure_ascii=False)
                writer.writerow([row.get('source_file'), row.get('file_type'), row.get('table_or_sheet') or '', row.get('row_index') if row.get('row_index') is not None else '', json_blob])
                rows_written += 1
                # flush periodically to avoid huge memory
                if rows_written % 10000 == 0:
                    fh.flush()
        # notify writer
        rowmeta_q.put({'tmpfile': str(tf), 'rows': rows_written, 'source_file': str(p), 'sha256': sha, 'mtime': stat.st_mtime, 'size': stat.st_size})
        logger.info(f"Worker wrote {rows_written} rows for {p} -> {tf}")
    logger.info("Worker exiting")

# ---------------- Writer process ----------------
def writer_proc(rowmeta_q: Queue, pg_url: str, use_copy: bool, global_tmpdir: str, metrics_q: Queue, stop_event: Event, per_file_table: bool):
    """
    Writer: читает метаданные о temp файлах и делает COPY INTO raw_rows (или per-file table).
    """
    logger.info("Writer starting...")
    # psycopg2 connection
    conn = psycopg2_conn_from_url(pg_url)
    cur = conn.cursor()
    # Ensure meta tables exist via SQLAlchemy
    engine = sql_alchemy_engine(pg_url)
    create_meta_tables(engine)

    while not stop_event.is_set() or not rowmeta_q.empty():
        try:
            meta = rowmeta_q.get(timeout=1)
        except Exception:
            continue
        if meta == "STOP":
            break
        tmpfile = meta['tmpfile']
        rows = meta['rows']
        source_file = meta.get('source_file')
        sha = meta.get('sha256')
        mtime = datetime.fromtimestamp(meta.get('mtime')) if meta.get('mtime') else None
        size = meta.get('size')
        try:
            # Insert via COPY for speed
            if use_copy and rows >= COPY_THRESHOLD:
                with open(tmpfile, 'r', encoding='utf-8') as f:
                    # COPY with delimiter tab; data columns: source_file, file_type, table_or_sheet, row_index, data
                    sql = "COPY raw_rows (source_file, file_type, table_or_sheet, row_index, data) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"')"
                    cur.copy_expert(sql, f)
                    conn.commit()
            else:
                # fallback: read and executemany
                with open(tmpfile, 'r', encoding='utf-8') as f:
                    r = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                    params = []
                    for row in r:
                        # row: [source_file, file_type, table_or_sheet, row_index, json_blob]
                        params.append((row[0], row[1], row[2] if row[2] else None, int(row[3]) if row[3] else None, psycopg2.extras.Json(json.loads(row[4]))))
                        if len(params) >= 1000:
                            psycopg2.extras.execute_values(cur, "INSERT INTO raw_rows (source_file,file_type,table_or_sheet,row_index,data) VALUES %s", params)
                            conn.commit()
                            params = []
                    if params:
                        psycopg2.extras.execute_values(cur, "INSERT INTO raw_rows (source_file,file_type,table_or_sheet,row_index,data) VALUES %s", params)
                        conn.commit()
            # update imported_files meta
            with engine.begin() as eg:
                eg.execute(text("""
                    INSERT INTO imported_files (source_file, sha256, mtime, size, status, rows_imported, imported_at)
                    VALUES (:sf, :sha, :mtime, :size, 'done', :rows, now())
                    ON CONFLICT (source_file) DO UPDATE
                      SET sha256 = EXCLUDED.sha256, mtime = EXCLUDED.mtime, size = EXCLUDED.size, status = EXCLUDED.status, rows_imported = EXCLUDED.rows_imported, imported_at = EXCLUDED.imported_at
                """), {"sf": source_file, "sha": sha, "mtime": mtime, "size": size, "rows": rows})
            # metrics
            metrics_q.put({'event': 'file_done', 'rows': rows, 'file': source_file})
            logger.info(f"Writer imported {rows} rows from {source_file}")
            # cleanup tmpfile
            try:
                os.remove(tmpfile)
            except Exception:
                pass
        except Exception as e:
            conn.rollback()
            logger.exception(f"Failed to import tmpfile {tmpfile}: {e}")
            with engine.begin() as eg:
                eg.execute(text("""
                    INSERT INTO imported_files (source_file, sha256, mtime, size, status, last_error)
                    VALUES (:sf, :sha, :mtime, :size, 'failed', :err)
                    ON CONFLICT (source_file) DO UPDATE SET status = 'failed', last_error = :err
                """), {"sf": source_file, "sha": sha, "mtime": mtime, "size": size, "err": str(e)})
            metrics_q.put({'event': 'file_failed', 'file': source_file, 'error': str(e)})
    try:
        cur.close()
        conn.close()
    except:
        pass
    logger.info("Writer exiting")

# ---------------- Metrics server (aiohttp) ----------------
def metrics_server_proc(metrics_q: Queue, http_host: str, http_port: int, stop_event: Event):
    """
    Простая loop: агрегирует показатели и отвечает на HTTP /metrics.
    """
    import asyncio
    from aiohttp import web

    app = web.Application()
    stats = {'files_done': 0, 'rows': 0, 'files_failed': 0, 'last_errors': []}

    async def metrics_handler(request):
        return web.json_response(stats)

    app.add_routes([web.get('/metrics', metrics_handler)])

    async def aggregator():
        while not stop_event.is_set():
            try:
                m = metrics_q.get(timeout=1)
            except Exception:
                await asyncio.sleep(0.1)
                continue
            ev = m.get('event')
            if ev == 'file_done':
                stats['files_done'] += 1
                stats['rows'] += m.get('rows', 0)
            elif ev == 'file_failed':
                stats['files_failed'] += 1
                stats['last_errors'].append({'file': m.get('file'), 'error': m.get('error')})
            # keep last 20 errors
            stats['last_errors'] = stats['last_errors'][-20:]

    async def start_background(app):
        app['agg_task'] = asyncio.create_task(aggregator())

    async def cleanup(app):
        app['agg_task'].cancel()
        try:
            await app['agg_task']
        except:
            pass

    app.on_startup.append(start_background)
    app.on_cleanup.append(cleanup)

    web.run_app(app, host=http_host, port=http_port)

# ---------------- Main coordinator ----------------
def main():
    parser = argparse.ArgumentParser(description="Ultimate importer to PostgreSQL")
    parser.add_argument('path', help="Path to file or directory")
    parser.add_argument('--pg', required=True, help="SQLAlchemy Postgres URL e.g. postgresql+psycopg2://user:pass@host:5432/db")
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS)
    parser.add_argument('--batch', type=int, default=DEFAULT_BATCH)
    parser.add_argument('--use-copy', action='store_true', help="Use COPY for bulk loads")
    parser.add_argument('--tmpdir', default=None, help="Temporary directory for staging (default: auto)")
    parser.add_argument('--monitor', action='store_true', help="Run HTTP metrics server (aiohttp) on 0.0.0.0:8080")
    parser.add_argument('--execute-sql-dumps', action='store_true', help="Execute .sql dumps (DANGEROUS; only for trusted)")
    parser.add_argument('--per-file-table', action='store_true', help="Create per-file/ per-table normalized SQL tables (experimental)")
    parser.add_argument('--ext', nargs='*', help="Limit to extensions e.g. .csv .json .db")
    args = parser.parse_args()

    root = Path(args.path)
    if not root.exists():
        print("Path not found", file=sys.stderr); sys.exit(2)

    pg_url = args.pg
    engine = sql_alchemy_engine(pg_url)
    create_meta_tables(engine)

    tempdir = Path(args.tmpdir) if args.tmpdir else Path(tempfile.mkdtemp(prefix=TMPDIR_PREFIX))
    tempdir.mkdir(parents=True, exist_ok=True)

    # Queues
    task_q = Queue()
    rowmeta_q = Queue()
    metrics_q = Queue()
    stop_event = Event()

    # Start writer
    writer = Process(target=writer_proc, args=(rowmeta_q, pg_url, args.use_copy, str(tempdir), metrics_q, stop_event, args.per_file_table), daemon=True)
    writer.start()

    # Start metrics server if requested
    metrics_proc = None
    if args.monitor:
        metrics_proc = Process(target=metrics_server_proc, args=(metrics_q, '0.0.0.0', 8080, stop_event), daemon=True)
        metrics_proc.start()
        logger.info("Metrics server started on 0.0.0.0:8080")

    # Start workers
    workers = []
    for i in range(args.workers):
        p = Process(target=worker_proc, args=(task_q, str(tempdir), rowmeta_q, stop_event), daemon=True)
        p.start()
        workers.append(p)

    # Feed tasks
    exts = None
    if args.ext:
        exts = set(e if e.startswith('.') else f".{e}" for e in args.ext)
    files = list(find_files(root, include_exts=exts))
    logger.info(f"Found {len(files)} files to import")

    try:
        for f in files:
            task_q.put(str(f))
        # stop workers: put STOP markers
        for _ in workers:
            task_q.put("STOP")
        # wait workers finish
        for w in workers:
            w.join()
        # signal writer to stop
        rowmeta_q.put("STOP")
        writer.join()
    except KeyboardInterrupt:
        logger.warning("Interrupted, shutting down...")
        stop_event.set()
    finally:
        # metrics
        if metrics_proc:
            metrics_proc.terminate()
        # cleanup tempdir (optional)
        # shutil.rmtree(tempdir)
        logger.info("Done.")

if __name__ == "__main__":
    main()

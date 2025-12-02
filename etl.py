#!/usr/bin/env python3
import os
import csv
import gzip
import json
import orjson
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from multiprocessing import Process, Queue, cpu_count
from tqdm import tqdm

# ============ CONFIG ============
DATA_DIR = "/opt/data"      # ПАПКА С ТВОИМИ 52 ПАПКАМИ И 570 ФАЙЛАМИ
BATCH_SIZE = 5000
WORKERS = max(2, cpu_count() - 1)

PG_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "bigdata",
    "user": "postgres",
    "password": "password"
}

TARGET_TABLE = "raw_import"
LOGFILE = "/opt/etl_loader/etl.log"
# =================================


# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()
    ]
)


# ---------- Database ----------
def connect_pg():
    return psycopg2.connect(**PG_CONFIG)


def create_table_if_missing():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        source_file TEXT,
        record JSONB
    );
    """
    with connect_pg() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def db_writer(row_queue: Queue):
    conn = connect_pg()
    cur = conn.cursor()
    buffer = []

    logging.info("💾 DB writer started")

    while True:
        item = row_queue.get()
        if item == "STOP":
            break

        buffer.append(item)

        if len(buffer) >= BATCH_SIZE:
            try:
                execute_values(
                    cur,
                    f"INSERT INTO {TARGET_TABLE} (source_file, record) VALUES %s",
                    buffer
                )
                conn.commit()
                buffer = []
            except Exception as e:
                logging.error(f"DB insert failed: {e}")

    if buffer:
        try:
            execute_values(
                cur,
                f"INSERT INTO {TARGET_TABLE} (source_file, record) VALUES %s",
                buffer
            )
            conn.commit()
        except Exception as e:
            logging.error(f"Final flush failed: {e}")

    cur.close()
    conn.close()
    logging.info("💾 DB writer stopped")


# ---------- File Parsers ----------
def parse_csv(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def parse_tsv(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            yield row


def parse_gz(path):
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                yield orjson.loads(line)
            except:
                pass


def parse_json(path):
    with open(path, "rb") as f:
        try:
            data = orjson.loads(f.read())
            if isinstance(data, list):
                for r in data:
                    yield r
            elif isinstance(data, dict):
                yield data
        except:
            pass


def parse_txt(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield {"line": line.strip()}


def parse_log(path):
    return parse_txt(path)


def parse_xlsx(path):
    df = pd.read_excel(path, engine="openpyxl")
    for _, row in df.iterrows():
        yield row.to_dict()


def parse_xls(path):
    df = pd.read_excel(path)
    for _, row in df.iterrows():
        yield row.to_dict()


def parse_sql(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield {"sql_line": line.strip()}


def parse_db(path):
    # Заглушка: можно расширить под sqlite и другие форматы
    yield {"raw_db_file": path}


PARSERS = {
    ".csv": parse_csv,
    ".tsv": parse_tsv,
    ".gz": parse_gz,
    ".json": parse_json,
    ".txt": parse_txt,
    ".log": parse_log,
    ".xlsx": parse_xlsx,
    ".xls": parse_xls,
    ".sql": parse_sql,
    ".db": parse_db,
    ".info": parse_txt
}


# ---------- Worker ----------
def worker(task_queue: Queue, row_queue: Queue):
    logging.info("Worker started")

    while True:
        path = task_queue.get()
        if path == "STOP":
            break

        ext = os.path.splitext(path)[1].lower()
        parser = PARSERS.get(ext)

        if not parser:
            logging.warning(f"Unknown format: {path}")
            continue

        try:
            for record in parser(path):
                row_queue.put((path, json.dumps(record, ensure_ascii=False)))
        except Exception as e:
            logging.error(f"Error parsing {path}: {e}")

    logging.info("Worker stopped")


# ---------- File Finder ----------
def find_files(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


# ---------- Main ----------
def main():
    create_table_if_missing()

    task_queue = Queue()
    row_queue = Queue()

    # writer process
    writer = Process(target=db_writer, args=(row_queue,))
    writer.start()

    # workers
    workers = [
        Process(target=worker, args=(task_queue, row_queue))
        for _ in range(WORKERS)
    ]
    for w in workers:
        w.start()

    all_files = list(find_files(DATA_DIR))
    logging.info(f"📁 Found {len(all_files)} files")

    for f in tqdm(all_files, desc="Processing files"):
        task_queue.put(f)

    for _ in workers:
        task_queue.put("STOP")

    for w in workers:
        w.join()

    row_queue.put("STOP")
    writer.join()

    logging.info("🎉 ETL Finished")


if __name__ == "__main__":
    main()

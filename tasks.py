import os
import time
import socket
import redis
import psutil
import random
from celery_app import app
import xmlrpc.client
from celery import group
from connector import authenticate
import openpyxl
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

_logger = logging.getLogger(__name__)


SN = "S/N"
IPPIS_NO = "IPPIS NUMBER"
NAME = "NAME"
PEN_NUMBER = "PEN NUMBER"
NOT_FOUND = []
READ_FIELDS = ["name", "pension_pin", "employee_no"]
ODOO_URL = os.getenv("ODOO_URL", "http://localhost:8069")
ODOO_DB = os.getenv("ODOO_DB", "naseni")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "admin")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "nas123@")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50))
MODEL_NAME = "hr.employee"
REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
REDIS_PROGRESS_KEY = "pen-update:hr.employee:progress"
REDIS_SUCCESS_LIST = "pen-update:hr.employee:success_records"
REDIS_FAILURE_LIST = "pen-update:hr.employee:failed_records"
REDIS_NOT_FOUND_LIST = "pen-update:hr.employee:not_found_records"
CHUNK_SIZE = 50
redis_client = None


def get_redis_client():
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis.from_url(REDIS_URL)
    return redis_client


def splittor(rs):
    chunks = []
    for idx in range(0, len(rs), CHUNK_SIZE):
        sub = rs[idx: idx + CHUNK_SIZE]
        chunks.append(sub)
    return chunks


def try_connect_with_retry(attempts=10, initial_delay=1, max_delay=30):
    for attempt in range(1, attempts + 1):
        try:
            return authenticate(
                ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
        except (
            socket.gaierror,
            ConnectionError,
            TimeoutError,
            xmlrpc.client.Error,
        ) as e:
            _logger.warning(f"[Attempt {attempt}] Odoo connection failed: {e}")
            if attempt == attempts:
                raise
            import random

            delay = min(initial_delay * (2 ** (attempt - 1)), max_delay)
            jittered_delay = delay * (0.5 + random.random() * 0.5)
            time.sleep(jittered_delay)


@app.task(bind=True, max_retries=5)
def process_chunk(self, chunk_rows):
    uid, models, db, password = try_connect_with_retry()
    redis_client = get_redis_client()

    def log_memory_usage(note=""):
        process = psutil.Process(os.getpid())
        _logger.info(
            f"[Memory] {note} - {process.memory_info().rss / 1024**2:.2f} MB")

    for idx, record in enumerate(chunk_rows, start=1):
        log_memory_usage(f"Before processing record {idx}")
        _logger.info(f"Processing record.........: {record}")
        if not record[0]:
            continue

        try:
            _logger.info(
                f"employee id: , {record[0]}, update vals: , {record[-1]}")
            models.execute_kw(
                db,
                uid,
                password,
                "hr.employee",
                "write",
                [[int(record[0])], record[-1]],
            )
            _logger.info(f"Processing record {idx}: {record}")
            redis_client.rpush(REDIS_SUCCESS_LIST, str(record))
        except Exception as exc:
            _logger.warning(f"Retrying due to error: {exc}")
            _logger.exception(
                f"Exception while creating record at index {idx}: {record}"
            )
            redis_client.rpush(REDIS_FAILURE_LIST, str(record))
            sleep_time = 0.2 + random.random() * 0.3
            time.sleep(sleep_time)
            raise process_chunk.retry(exc=exc, countdown=5)


@app.task
def main():
    uid, models, db, password = authenticate(
        ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD
    )

    wb = openpyxl.load_workbook("./data/pension_data.xlsx", read_only=True)
    sheet = wb.active

    all_rows_iter = sheet.iter_rows(min_row=2, values_only=True)
    all_rows = [
        row
        for row in all_rows_iter
        if any(
            cell is not None and (
                not (isinstance(cell, str) and cell.strip() == ""))
            for cell in row
        )
    ]

    values_to_update = []

    for _, IPPIS_NO, NAME, PEN_NUMBER in all_rows[:10]:
        if IPPIS_NO is None:
            _logger.warning(
                f"Skipping row with missing employee number: {NAME}")
            continue

        try:
            [emp_id] = models.execute_kw(
                db,
                uid,
                password,
                "hr.employee",
                "search",
                [[["employee_no", "=", str(IPPIS_NO).strip()]]],
            )
        except ValueError:
            _logger.info(f"IPPIS No {IPPIS_NO} not found")
            NOT_FOUND.append(IPPIS_NO)
            continue

        _logger.info(f"Processing Employee No: {IPPIS_NO}, Name: {NAME}")

        [employee_details] = models.execute_kw(
            db,
            uid,
            password,
            "hr.employee",
            "read",
            [int(emp_id)],
            {"fields": READ_FIELDS},
        )

        current_pin = employee_details.get("pension_pin", "")
        update_vals = {"pension_pin": PEN_NUMBER or current_pin or ""}

        values_to_update.append((emp_id, update_vals))

    job = group(process_chunk.s(chunk) for chunk in splittor(values_to_update))

    job.apply_async()

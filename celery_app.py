import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = Celery(main="naseni-tasks", backend=REDIS_URL, broker=REDIS_URL)

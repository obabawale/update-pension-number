FROM python:3.12-slim

RUN useradd -m -s /bin/bash celery

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chown -R celery:root /app

USER celery

CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]

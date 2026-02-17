# Koyeb-ready container (no code changes inside bot.py)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bot.py /app/bot.py
COPY runner.py /app/runner.py

# Koyeb sets PORT for web services, but this bot is a worker (polling). No port is required.
CMD ["python", "runner.py"]

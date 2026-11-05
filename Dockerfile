# Rewind API image (Phase 1).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rewind ./rewind
COPY scripts ./scripts

EXPOSE 8010

# Bind to $PORT when the platform injects one (Render/Railway/Fly), else 8010.
CMD ["sh", "-c", "uvicorn rewind.api.main:app --host 0.0.0.0 --port ${PORT:-8010}"]

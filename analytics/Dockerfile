FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
COPY --from=builder /install /usr/local
RUN useradd --create-home appuser
WORKDIR /app
COPY --chown=appuser:appuser . .
USER appuser
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]

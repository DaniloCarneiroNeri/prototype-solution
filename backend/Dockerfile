# --- Build Stage ---
FROM python:3.11-slim AS builder
WORKDIR /app

# Copia apenas dependências primeiro
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Final Image ---
FROM python:3.11-slim
WORKDIR /app

# Copia libs instaladas
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11

# Copia o código da aplicação
COPY . .

EXPOSE 8000

# FastAPI com Uvicorn (produção)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

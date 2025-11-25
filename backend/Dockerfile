# Stage 1 – instalar dependências
FROM python:3.11-slim AS builder
WORKDIR /app

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# Stage 2 – imagem final
FROM python:3.11-slim
WORKDIR /app

# Copia somente as dependências instaladas corretamente
COPY --from=builder /install /usr/local

# Copia o código do backend
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
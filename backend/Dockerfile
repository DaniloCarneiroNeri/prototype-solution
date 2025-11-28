# Stage 1 – Builder
FROM python:3.11-slim AS builder
WORKDIR /app

# Variáveis para evitar cache desnecessário e garantir logs imediatos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --prefix=/install --no-cache-dir -r requirements.txt && \
    pip install --prefix=/install --no-cache-dir openai openpyxl python-multipart

# Stage 2 – Imagem Final
FROM python:3.11-slim
WORKDIR /app

# Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copia as bibliotecas instaladas no stage anterior
COPY --from=builder /install /usr/local

# Copia o código fonte
COPY . .

# Expõe a porta
EXPOSE 8000

# Comando de inicialização
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
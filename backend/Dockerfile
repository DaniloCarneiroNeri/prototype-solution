# Stage 1 – Builder
FROM python:3.11-slim AS builder
WORKDIR /app

# dependências necessárias para bcrypt + cryptography
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt


# Stage 2 – Final image
FROM python:3.11-slim
WORKDIR /app

# instalar dependências de runtime
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# copiar libs instaladas (usuário root do builder)
COPY --from=builder /root/.local /root/.local

# garantir que python use libs instaladas
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/root/.local:$PYTHONPATH

# copiar código fonte
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

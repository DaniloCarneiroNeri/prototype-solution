# =============================
# Base image (não usar Alpine)
# =============================
FROM node:18-slim

# Cria usuário sem privilégios (melhor prática)
RUN addgroup --system app && adduser --system --group app

WORKDIR /app

# Copia apenas os manifestos primeiro (cache melhor)
COPY package.json package-lock.json* ./

# Instala dependências (prod + dev se necessário)
RUN npm ci

# Copia o restante do projeto
COPY . .

# Permissões
RUN chown -R app:app /app
USER app

EXPOSE 3000

# Healthcheck recomendado pelo Render
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s \
  CMD node -e "require('http').get('http://localhost:3000', r => process.exit(0)).on('error', e => process.exit(1))"

# Comando inicial
CMD ["node", "server.js"]

# frontend/Dockerfile
FROM node:18-slim

# cria usuário sem permissão root
RUN addgroup --system app && adduser --system --group app

WORKDIR /app

# copia manifestos primeiro
COPY package.json package-lock.json* ./

# instala TODAS as dependências (sem --only=production)
RUN npm ci

# copia restante do projeto
COPY . .

RUN chown -R app:app /app
USER app

EXPOSE 3000

CMD ["node", "server.js"]

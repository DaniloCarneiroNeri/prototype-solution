# frontend/Dockerfile
FROM node:18-slim

# criar usuário sem permissões
RUN addgroup --system app && adduser --system --group app

WORKDIR /app

# copiar manifestos
COPY package.json package-lock.json* ./

# instala todas as dependências corretamente
RUN npm install

# copia o restante do código
COPY . .

RUN chown -R app:app /app
USER app

EXPOSE 3000

CMD ["node", "server.js"]

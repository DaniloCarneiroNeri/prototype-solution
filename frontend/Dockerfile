# frontend/Dockerfile
# (path: frontend/Dockerfile)
FROM node:18-alpine

# Use a non-root user (safer)
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app

# Copia apenas deps primeiro para cache eficaz
COPY package.json package-lock.json* ./

# Instala apenas prod dependencies
RUN npm ci --only=production

# Copia o resto do código
COPY . .

# Ajusta permissões e usuário
RUN chown -R app:app /app
USER app

EXPOSE 3000

# Start do seu servidor Node (server.js)
CMD ["node", "server.js"]
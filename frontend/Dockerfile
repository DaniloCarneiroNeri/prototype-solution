# --- Build Stage ---
FROM node:18 AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm install

COPY . .
RUN npm run build   # <-- gera /build

# --- Final Image (Nginx) ---
FROM nginx:stable-alpine
WORKDIR /usr/share/nginx/html

# Apaga conteúdo padrão
RUN rm -rf ./*

# Copia arquivos gerados pelo build
COPY --from=build /app/build .

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]

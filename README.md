# prototype-solution

Plano (pseudocódigo / passos)

Criar conexão assíncrona com Postgres usando DATABASE_URL (usado pelo Render).

Criar modelo User (id, email, hashed_password, created_at).

Criar endpoints /auth/register e /auth/login:

register: valida email, faz hash via bcrypt (passlib), salva usuário.

login: valida credenciais e retorna JWT (python-jose).

No startup do FastAPI, executar Base.metadata.create_all (cria tabela users se não existir) — abordagem leve (sem migrations) para reduzir impacto.

Atualizar requirements.txt do backend para incluir dependências necessárias.

No frontend Node, adicionar rotas proxy /api/auth/register e /api/auth/login que repassam para o backend (leitura via PYTHON_AUTH env var). Criar public/register.html e public/login.html para testar.

Dockerfile já usa requirements.txt, então após atualizar requirements.txt basta rebuildar a imagem do backend.

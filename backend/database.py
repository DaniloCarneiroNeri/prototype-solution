import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL não configurada!")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

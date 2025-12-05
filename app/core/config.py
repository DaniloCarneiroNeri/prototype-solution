import os

class Settings:
    HERE_API_KEY = os.getenv("HERE_API_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    MAX_CONCURRENT_REQUESTS = 10 

settings = Settings()
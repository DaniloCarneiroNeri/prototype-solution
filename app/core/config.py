import os

class Settings:
    HERE_API_KEY = os.getenv("HERE_API_KEY")
    MAX_CONCURRENT_REQUESTS = 10 

settings = Settings()
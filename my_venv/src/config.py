from dotenv import load_dotenv
from pydantic import ValidationError, Field
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DATABASE_URL: str
    RABBITMQ_URL: str
    REDIS_URL: str


    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = "ignore"



    @property
    def get_db_url(self):
        return (f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@"
                f"{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")
        # return self.DATABASE_URL

try:
    settings = Settings()
except ValidationError as e:
    print(f"Configuration validation error: {e}")
    exit(1)
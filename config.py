from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # This must match the environment variable name in docker-compose or .env
    database_url: str = "postgresql://user:pass@localhost:5432/db"
    app_url: str = "aac.itcollege.ee"
    log_level: str = "info"
    upload_dir: str = "uploads"

    jwt_secret: str = ""

    smtp_server: str = ""
    smtp_port: int = 0
    smtp_username: str = ""
    smtp_password: str = ""
    sender_email: str = ""

    default_pin: str = "9999"

    storage_type: str = "CLOUDFLARE"
    r2_bucket_name: str = ""
    r2_account_id: str = ""
    r2_access_key: str = ""
    r2_secret_key: str = ""
    r2_public_url: str = ""

    # Tell Pydantic to look for a .env file locally, 
    # but in Docker, system env vars will take priority.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

# Create a singleton instance to use across your app
settings = Settings()
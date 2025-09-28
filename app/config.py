import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    # Botmaker
    botmaker_base_url: str = os.getenv("BOTMAKER_BASE_URL", "https://api.botmaker.com/v2.0")
    botmaker_api_token: str = os.getenv("BOTMAKER_API_TOKEN", "")
    botmaker_business_id: str = os.getenv("BOTMAKER_BUSINESS_ID", "")

    # Chatwoot
    chatwoot_base_url: str = os.getenv("CHATWOOT_BASE_URL", "https://app.chatwoot.com")
    chatwoot_api_access_token: str = os.getenv("CHATWOOT_API_ACCESS_TOKEN", "")
    chatwoot_account_id: str = os.getenv("CHATWOOT_ACCOUNT_ID", "")
    chatwoot_inbox_id: str = os.getenv("CHATWOOT_INBOX_ID", "")

    # Storage
    storage_backend: str = os.getenv("STORAGE_BACKEND", "local")  # local | supabase
    data_dir: str = os.getenv("DATA_DIR", "/app/data")
    log_dir: str = os.getenv("LOG_DIR", "/app/logs")
    mappings_dir: str = os.getenv("MAPPINGS_DIR", "/app/mappings")

    # Supabase (optional)
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_anon_key: str = os.getenv("SUPABASE_ANON_KEY", "")
    supabase_service_role_key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    supabase_bucket: str = os.getenv("SUPABASE_BUCKET", "botmaker-raw")
    supabase_schema: str = os.getenv("SUPABASE_SCHEMA", "public")
    supabase_db_url: str = os.getenv("SUPABASE_DB_URL", "")

    # Tuning
    rate_limit_rps: float = float(os.getenv("RATE_LIMIT_RPS", "4"))
    page_size: int = int(os.getenv("PAGE_SIZE", "100"))
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "200"))

    # Extraction window
    extract_start: str | None = os.getenv("EXTRACT_START")
    extract_end: str | None = os.getenv("EXTRACT_END")


def get_settings() -> Settings:
    return Settings()

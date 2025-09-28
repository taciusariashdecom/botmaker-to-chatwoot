# Technical Requirements Checklist

- [ ] `.env.example` includes all necessary environment variables (Botmaker, Chatwoot, storage, tuning). ✅ after verification
- [ ] `app/config.py` loads config values with sane defaults.
- [ ] `app/http.py` handles rate limiting and retry with Tenacity for both APIs.
- [ ] `app/botmaker.py` exposes `stream_chats` and `stream_messages` with pagination.
- [ ] `app/extract.py` writes NDJSON files and updates checkpoints.
- [ ] `app/load.py` performs idempotent import, updating mapping stores and export flags.
- [ ] `app/storage.py` supports local backend for read/write of NDJSON and binary assets.
- [ ] Mapping stores (`app/mapping_store.py`) and checkpoints (`app/checkpoints.py`) persist JSON safely.
- [ ] `app/chatwoot.py` wraps Application API endpoints for contacts, conversations, and messages.
- [ ] Logging configured with console + rotating file handler (`app/logging_setup.py`).
- [ ] Dockerfile builds Python runtime, `docker-compose.yml` runs extractor/loader with volumes.
- [ ] Tests in `tests/` cover storage, checkpoints, payload helpers, and mapping logic.
- [ ] README documents setup, extraction, loading, troubleshooting, and reruns.
- [ ] Requirements checklist (`docs/requirements-checklist.md`) kept updated.

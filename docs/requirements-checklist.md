# Requirements Checklist

Use this document to track the project requirements. Mark a checkbox only after verifying the item via test or manual validation.

## High-Level Features

- [ ] Extract Botmaker contacts, chats, and messages to NDJSON with checkpoints (`app/extract.py`).
- [ ] Track contact export status fields (`inserted_at`, `exported_to_chatwoot`, `exported_at`).
- [ ] Load contacts into Chatwoot with idempotent mapping (`app/load.py`).
- [ ] Load conversations into Chatwoot with idempotent mapping (`app/load.py`).
- [ ] Replay messages into Chatwoot with idempotent mapping (`app/load.py`).
- [ ] Persist export/import status snapshots alongside source data (`data/<prefix>/*_export_status.ndjson`).
- [ ] Provide command-line flags for dry-run, limits, and checkpoint resets.
- [ ] Document setup and runbook in `README.md`.
- [ ] Automated tests covering storage, checkpoints, and payload builders (`tests/`).

## Operational Requirements

- [ ] Dockerized execution for extractor and loader (`docker-compose.yml`).
- [ ] Rate limiting and retry support for Botmaker/Chatwoot clients (`app/http.py`).
- [ ] Mapping stores persisted to `mappings/` (`contact_map.json`, `conversation_map.json`, `message_map.json`).
- [ ] Logged progress and metrics (log rotation via `app/logging_setup.py`).
- [ ] Checklist documents kept up-to-date (this file).

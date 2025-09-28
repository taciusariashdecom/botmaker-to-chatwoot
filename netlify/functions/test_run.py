from __future__ import annotations

import json
import logging
import sys
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.config import get_settings
from app.extract import run_sample_extract
from app.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _json_response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(payload),
    }


def handler(event, context):  # noqa: D401 (Netlify signature)
    method = (event or {}).get("httpMethod", "GET").upper()

    if method == "OPTIONS":
        return _json_response(HTTPStatus.NO_CONTENT, {})

    if method not in {"GET", "POST"}:
        return _json_response(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "Method not allowed"})

    settings = get_settings()

    if not settings.botmaker_api_token:
        return _json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "error": "Credenciais incompletas",
                "hint": "Defina BOTMAKER_API_TOKEN (e, se necessário, BOTMAKER_BASE_URL) nas variáveis de ambiente do Netlify.",
            },
        )

    setup_logging(settings.log_dir)

    try:
        result = run_sample_extract(
            settings=settings,
            max_chats=1,
            messages_per_chat=1,
            skip_messages=False,
            long_term=False,
        )
        return _json_response(HTTPStatus.OK, result)
    except ValueError as exc:
        logger.exception("Erro de validação ao executar teste rápido")
        return _json_response(
            HTTPStatus.BAD_REQUEST,
            {
                "error": "Erro ao consultar Botmaker",
                "details": str(exc),
                "hint": "Confirme se o token informado possui acesso de leitura às APIs /chats e /messages.",
            },
        )
    except Exception as exc:  # noqa: BLE001 - retorno amigável
        logger.exception("Falha inesperada na função de teste")
        return _json_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {
                "error": "Falha inesperada ao executar teste rápido",
                "details": str(exc),
            },
        )

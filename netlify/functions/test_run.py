from __future__ import annotations

import logging
import sys
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Tuple

import json
from datetime import date, datetime

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.config import get_settings
from app.extract import run_sample_extract
from app.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return list(value)
    return str(value)


def _json_response(status: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(payload, default=_json_default, ensure_ascii=False),
    }


def _http_error_to_payload(exc: httpx.HTTPStatusError) -> Tuple[int, Dict[str, Any]]:
    status_code = exc.response.status_code if exc.response else None
    detail = exc.response.text if exc.response else str(exc)

    if status_code == HTTPStatus.UNAUTHORIZED:
        return (
            HTTPStatus.UNAUTHORIZED,
            {
                "error": "Credenciais inválidas para a API Botmaker.",
                "details": detail,
                "hint": "Confirme o BOTMAKER_API_TOKEN informado no Netlify e gere um novo token se necessário.",
            },
        )

    if status_code == HTTPStatus.FORBIDDEN:
        return (
            HTTPStatus.FORBIDDEN,
            {
                "error": "Token não possui acesso ao business configurado.",
                "details": detail,
                "hint": "Garanta que o token esteja vinculado ao mesmo Business ID utilizado por este projeto.",
            },
        )

    if status_code == HTTPStatus.TOO_MANY_REQUESTS:
        return (
            HTTPStatus.TOO_MANY_REQUESTS,
            {
                "error": "Limite de requisições da API Botmaker atingido.",
                "details": detail,
                "hint": "Reduza RATE_LIMIT_RPS ou aguarde alguns segundos antes de tentar novamente.",
            },
        )

    if status_code == HTTPStatus.NOT_FOUND:
        return (
            HTTPStatus.BAD_GATEWAY,
            {
                "error": "Endpoint da API Botmaker não encontrado.",
                "details": detail,
                "hint": "Verifique se BOTMAKER_BASE_URL está correto e inclui /v2.0.",
            },
        )

    return (
        HTTPStatus.BAD_GATEWAY,
        {
            "error": "A API Botmaker retornou um erro inesperado.",
            "details": detail,
        },
    )


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

    resolved_log_dir = setup_logging(settings.log_dir)
    logger.info("Test run logging directory: %s", resolved_log_dir)

    try:
        result = run_sample_extract(
            settings=settings,
            max_chats=1,
            messages_per_chat=50,
            skip_messages=False,
            long_term=False,
        )
        if not result.get("chats"):
            return _json_response(
                HTTPStatus.NOT_FOUND,
                {
                    "error": "Nenhuma conversa retornada no intervalo padrão.",
                    "hint": "Confirme se existem conversas recentes no Botmaker ou ajuste EXTRACT_START/END.",
                },
            )
        return _json_response(HTTPStatus.OK, result)
    except httpx.HTTPStatusError as exc:
        logger.exception("Erro HTTP da API Botmaker durante teste rápido")
        status, payload = _http_error_to_payload(exc)
        return _json_response(status, payload)
    except httpx.RequestError as exc:
        logger.exception("Falha de rede ao consultar Botmaker")
        return _json_response(
            HTTPStatus.BAD_GATEWAY,
            {
                "error": "Não foi possível se conectar à API Botmaker.",
                "details": str(exc),
                "hint": "Cheque conectividade da função serverless e se o endpoint está acessível.",
            },
        )
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

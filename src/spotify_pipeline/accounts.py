import json
import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class SpotifyAccount:
    tenant_id: str
    spotify_user_id: str
    spotify_token: str


def load_spotify_accounts() -> List[SpotifyAccount]:
    """
    Загружает учётные данные tenant/user из переменных окружения.

    Поддерживаемые варианты:
    - SPOTIFY_ACCOUNTS_JSON: JSON-массив объектов {tenant_id, spotify_user_id, spotify_token}
    - запасной вариант single-tenant: TENANT_ID (опционально), SPOTIFY_USER_ID, SPOTIFY_TOKEN
    """

    raw = os.getenv("SPOTIFY_ACCOUNTS_JSON")
    if raw:
        parsed = json.loads(raw)
        accounts: List[SpotifyAccount] = []
        for a in parsed:
            tenant_id = a.get("tenant_id") or a.get("tenantId") or "default"
            spotify_user_id = a.get("spotify_user_id") or a.get("spotifyUserId") or a.get("user_id")
            spotify_token = a.get("spotify_token") or a.get("spotifyToken") or a.get("token")
            if not spotify_user_id or not spotify_token:
                raise ValueError(f"Invalid account entry in SPOTIFY_ACCOUNTS_JSON: {a}")
            accounts.append(
                SpotifyAccount(
                    tenant_id=str(tenant_id),
                    spotify_user_id=str(spotify_user_id),
                    spotify_token=str(spotify_token),
                )
            )
        if not accounts:
            raise ValueError("SPOTIFY_ACCOUNTS_JSON parsed to an empty list.")
        return accounts

    spotify_user_id = os.getenv("SPOTIFY_USER_ID")
    spotify_token = os.getenv("SPOTIFY_TOKEN")
    if not spotify_user_id or not spotify_token:
        raise ValueError(
            "Не найдены учетные данные Spotify. Укажите SPOTIFY_ACCOUNTS_JSON или (SPOTIFY_USER_ID + SPOTIFY_TOKEN)."
        )

    tenant_id = os.getenv("TENANT_ID", "default")
    return [SpotifyAccount(tenant_id=tenant_id, spotify_user_id=spotify_user_id, spotify_token=spotify_token)]


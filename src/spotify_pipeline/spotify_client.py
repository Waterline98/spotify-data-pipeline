import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


load_dotenv()


SPOTIFY_RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"


def _parse_spotify_dt(dt_str: str) -> datetime:
    # Spotify обычно возвращает ISO 8601 вида 2024-01-01T12:34:56Z
    # fromisoformat поддерживает формат с Z только через replace.
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(dt_str)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_unix_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _event_hash(tenant_id: str, spotify_user_id: str, played_at: datetime, track_id: str) -> str:
    base = f"{tenant_id}|{spotify_user_id}|{played_at.isoformat()}|{track_id}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SpotifyListenEvent:
    tenant_id: str
    spotify_user_id: str
    event_hash: str
    played_at: datetime
    cursor_after: int
    track_id: str
    track_name: str
    artist_ids: List[str]
    artist_names: List[str]
    raw_payload_json: str


def extract_recently_played(
    *,
    tenant_id: str,
    spotify_user_id: str,
    spotify_token: str,
    after_unix_ms: int,
    limit: int = 50,
    timeout_s: int = 20,
    max_retries: int = 6,
) -> Tuple[List[SpotifyListenEvent], Optional[int]]:
    """
    Извлекает события "recently played" из Spotify API, используя курсор `after`.

    Возвращает:
      - events: список нормализованных событий (одна строка на прослушивание трека, все артисты в массивах)
      - new_cursor_after_ms: максимальное значение played_at в unix-ms из полученных событий (для инкремента)
    """

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {spotify_token}",
    }
    url = f"{SPOTIFY_RECENTLY_PLAYED_URL}?limit={limit}&after={after_unix_ms}"

    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_s)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else min(2 ** attempt, 60)
                time.sleep(sleep_s)
                continue

            resp.raise_for_status()
            data = resp.json()
            items = data.get("items") or []
            events: List[SpotifyListenEvent] = []

            max_played_at_ms: Optional[int] = None
            for item in items:
                track = (item or {}).get("track") or {}
                played_at_str = item.get("played_at")
                if not played_at_str:
                    continue

                track_id = track.get("id")
                track_name = track.get("name") or ""
                if not track_id:
                    continue

                played_at = _parse_spotify_dt(played_at_str)
                played_at_ms = _to_unix_ms(played_at)

                # Track artists for "count by all artists"
                # Spotify usually has `track.artists: [{id, name}, ...]`
                artists = track.get("artists") or []
                if not artists:
                    # Fallback: album artists (then take all from album artists if present)
                    album_artists = ((track.get("album") or {}) or {}).get("artists") or []
                    artists = album_artists

                artist_ids: List[str] = []
                artist_names: List[str] = []
                for a in artists:
                    if not a:
                        continue
                    aid = a.get("id")
                    aname = a.get("name")
                    if aid:
                        artist_ids.append(str(aid))
                    if aname:
                        artist_names.append(str(aname))

                # Ensure both arrays have the same length for downstream arrayZip().
                # If names are missing, still keep positions aligned as empty strings.
                if len(artist_names) < len(artist_ids):
                    artist_names = artist_names + [""] * (len(artist_ids) - len(artist_names))

                event = SpotifyListenEvent(
                    tenant_id=str(tenant_id),
                    spotify_user_id=str(spotify_user_id),
                    event_hash=_event_hash(str(tenant_id), str(spotify_user_id), played_at, str(track_id)),
                    played_at=played_at,
                    cursor_after=int(after_unix_ms),
                    track_id=str(track_id),
                    track_name=str(track_name),
                    artist_ids=artist_ids,
                    artist_names=artist_names,
                    raw_payload_json=json.dumps(item, ensure_ascii=False),
                )
                events.append(event)

                if max_played_at_ms is None or played_at_ms > max_played_at_ms:
                    max_played_at_ms = played_at_ms

            return events, max_played_at_ms

        except requests.exceptions.RequestException as e:
            last_err = e
            # экспоненциальный backoff для сетевых ошибок / 5xx
            time.sleep(min(2 ** attempt, 60))
        except Exception as e:
            # Ошибки логики/парсинга ретраями обычно не лечатся.
            last_err = e
            break

    raise RuntimeError(f"Failed to extract Spotify recently played after retries. Last error: {last_err}")


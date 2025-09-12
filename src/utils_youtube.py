from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional, Dict

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

__all__ = ["extract_video_id", "fetch_youtube_meta"]

# Patrones comunes de IDs: watch?v=, youtu.be/, shorts/, embed/
_YT_ID_RE = re.compile(
    r"(?:youtu\.be/|youtube\.com/(?:watch\?v=|shorts/|embed/))([A-Za-z0-9_-]{11})",
    re.IGNORECASE,
)


def extract_video_id(url: Optional[str]) -> Optional[str]:
    """
    Extrae el video_id (11 chars) desde una URL de YouTube.
    Devuelve None si no se encuentra.
    """
    if not url:
        return None
    m = _YT_ID_RE.search(url)
    return m.group(1) if m else None


def _to_iso_date(upload_date: Optional[str], timestamp: Optional[int]) -> Optional[str]:
    """
    Normaliza fecha a 'YYYY-MM-DD' desde:
        - upload_date (YYYYMMDD) si existe
        - timestamp unix (segundos) si existe
    """
    if upload_date and len(upload_date) == 8 and upload_date.isdigit():
        return datetime.strptime(upload_date, "%Y%m%d").date().isoformat()
    if timestamp:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
    return None


def fetch_youtube_meta(url: str) -> Dict[str, Optional[str]]:
    """
    Obtiene metadatos básicos del vídeo usando yt-dlp sin descargar:
        - video_id
        - title
        - channel
        - publish_date (YYYY-MM-DD)
        - license

    Puede lanzar ValueError si no se consigue determinar un video_id.
    """
    auto = {}
    try:
        # 'quiet' y 'no_warnings' evitan logs ruidosos; no descargamos media.
        with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(url, download=False)
        video_id = info.get("id") or extract_video_id(url)
        if not video_id:
            raise ValueError(
                "No se pudo determinar youtube.video_id a partir de la URL."
            )

        # Preferimos 'upload_date'; si no, caemos a 'timestamp' (unix)
        publish_date = _to_iso_date(
            info.get("upload_date"),
            info.get("timestamp"),
        )

        auto = {
            "video_id": video_id,
            "title": info.get("title"),
            "channel": info.get("uploader") or info.get("channel"),
            "publish_date": publish_date,
            "license": info.get("license"),
        }
    except DownloadError as e:
        # yt-dlp no pudo extraer metadatos (privado, eliminado, restricción, etc.)
        vid = extract_video_id(url)
        if not vid:
            raise ValueError(f"No se pudo extraer el video_id: {e}") from e
        # Devolvemos lo mínimo posible con el ID, para decidir qué hacer.
        auto = {
            "video_id": vid,
            "title": None,
            "channel": None,
            "publish_date": None,
            "license": None,
        }
    return auto

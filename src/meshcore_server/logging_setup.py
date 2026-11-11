import logging
import sys


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    # Knihovna meshcore je v DEBUG velmi upovídaná; její ladicí výpis
    # zapneme jen tehdy, když je celý server v DEBUG.
    logging.getLogger("alembic").setLevel(logging.WARNING)
    # uvicorn by do logu psal celé URL WebSocketů včetně ?token=…; vlastní
    # záznamy o připojení píše server sám (bez tokenu).
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("meshcore").setLevel(
        logging.DEBUG if level.upper() == "DEBUG" else logging.WARNING
    )

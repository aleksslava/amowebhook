import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def cleanup_generated_file(file_path: str | Path) -> None:
    path = Path(file_path)
    try:
        path.unlink(missing_ok=True)
    except OSError as error:
        logger.warning(f"Failed to remove generated file {path}: {error}")

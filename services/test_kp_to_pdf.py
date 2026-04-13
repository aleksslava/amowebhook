from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from jinja2 import Environment, FileSystemLoader, select_autoescape


class _TemplateRequestShim:
    """Minimal shim for `request.url_for()` used inside the KP template."""

    def __init__(self, templates_dir: Path) -> None:
        self._templates_dir = templates_dir

    def url_for(self, route_name: str, **_: Any) -> str:
        if route_name != "kp_image":
            raise KeyError(f"Unsupported route name for template rendering: {route_name}")

        image_path = (self._templates_dir / "img01.png").resolve()
        if not image_path.exists():
            raise FileNotFoundError(f"Template image not found: {image_path}")
        return image_path.as_uri()


def _is_snap_wrapped_binary(binary_path: str) -> bool:
    path_obj = Path(binary_path)
    try:
        resolved = path_obj.resolve()
    except OSError:
        resolved = path_obj

    normalized = str(resolved).replace("\\", "/")
    if normalized.startswith("/snap/"):
        return True

    try:
        if path_obj.is_file() and path_obj.stat().st_size <= 32 * 1024:
            content = path_obj.read_text(encoding="utf-8", errors="ignore")
            if "snap run" in content or "snap-confine" in content:
                return True
    except OSError:
        return False

    return False


def _find_chromium_binaries() -> list[str]:
    env_candidates = (
        os.getenv("CHROMIUM_PATH"),
        os.getenv("CHROME_PATH"),
        os.getenv("EDGE_PATH"),
    )
    discovered: list[str] = []
    for candidate in env_candidates:
        if candidate and Path(candidate).exists():
            discovered.append(str(Path(candidate)))

    binary_names = (
        "google-chrome",
        "google-chrome-stable",
        "chrome",
        "msedge",
        "microsoft-edge",
        "brave-browser",
        "brave",
        "chromium",
        "chromium-browser",
    )
    for binary_name in binary_names:
        found_path = shutil.which(binary_name)
        if found_path:
            discovered.append(found_path)

    windows_candidates = (
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    )
    for candidate in windows_candidates:
        if Path(candidate).exists():
            discovered.append(candidate)

    unique_discovered: list[str] = []
    seen: set[str] = set()
    for candidate in discovered:
        if candidate in seen:
            continue
        unique_discovered.append(candidate)
        seen.add(candidate)

    if unique_discovered:
        return sorted(unique_discovered, key=_is_snap_wrapped_binary)

    raise FileNotFoundError(
        "Chromium executable not found. Set CHROMIUM_PATH (or CHROME_PATH) "
        "or install Chrome/Chromium/Edge."
    )


def _run_chromium_pdf_export(
    chromium_bin: str,
    html_uri: str,
    pdf_output_path: Path,
    work_dir: Path,
) -> None:
    runtime_dir = work_dir / "xdg_runtime"
    profile_dir = work_dir / "chromium_profile"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(runtime_dir, 0o700)

    env = os.environ.copy()
    env.setdefault("XDG_RUNTIME_DIR", str(runtime_dir))

    common_flags = [
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--allow-file-access-from-files",
        f"--user-data-dir={profile_dir}",
        "--no-pdf-header-footer",
        "--print-to-pdf-no-header",
        f"--print-to-pdf={pdf_output_path}",
        html_uri,
    ]

    attempt_errors: list[str] = []
    for retry_idx in range(3):
        for headless_mode in ("--headless=new", "--headless"):
            command = [chromium_bin, headless_mode, *common_flags]
            completed = subprocess.run(command, capture_output=True, text=True, env=env)
            if completed.returncode == 0 and pdf_output_path.exists() and pdf_output_path.stat().st_size > 0:
                return

            stderr_text = (completed.stderr or "").strip()
            stdout_text = (completed.stdout or "").strip()
            attempt_errors.append(
                f"retry={retry_idx + 1} {headless_mode}: returncode={completed.returncode}; "
                f"stderr={stderr_text or '<empty>'}; stdout={stdout_text or '<empty>'}"
            )

            if "is not a snap cgroup for tag" in stderr_text:
                raise RuntimeError(
                    "Detected snap cgroup isolation error. "
                    "Use a non-snap Chromium/Chrome binary (for example via CHROMIUM_PATH). "
                    + " | ".join(attempt_errors)
                )

        if retry_idx < 2:
            time.sleep(1.0 + retry_idx)

    raise RuntimeError("Failed to export PDF via Chromium. " + " | ".join(attempt_errors))


def _format_grouped_number(value: int | float | str | None) -> str:
    if value in (None, ""):
        return ""

    raw_value = str(value).replace(" ", "").replace(",", ".")
    try:
        number = Decimal(raw_value)
    except (InvalidOperation, ValueError):
        return str(value)

    normalized = format(number.normalize(), "f")
    sign = ""
    if normalized.startswith("-"):
        sign = "-"
        normalized = normalized[1:]

    if "." in normalized:
        integer_part, fraction_part = normalized.split(".", 1)
        fraction_part = fraction_part.rstrip("0")
    else:
        integer_part, fraction_part = normalized, ""

    grouped_integer = f"{int(integer_part):,}".replace(",", " ")
    if fraction_part:
        return f"{sign}{grouped_integer},{fraction_part}"
    return f"{sign}{grouped_integer}"


def render_template_to_pdf(
    template_path: str | Path,
    context: Mapping[str, Any],
    output_pdf_path: str | Path | None = None,
) -> Path:
    """
    Render an HTML template with a context dict and convert it to PDF via headless Chromium.

    Args:
        template_path: Path to HTML template (for example, services/templates/test.html).
        context: Data for Jinja template rendering.
        output_pdf_path: Optional resulting PDF file path. Defaults to template name with .pdf.

    Returns:
        Absolute path to the created PDF file.
    """

    resolved_template_path = Path(template_path).resolve()
    if not resolved_template_path.exists():
        raise FileNotFoundError(f"Template file not found: {resolved_template_path}")

    templates_dir = resolved_template_path.parent
    jinja_env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    jinja_env.filters.setdefault("grouped_number", _format_grouped_number)
    template = jinja_env.get_template(resolved_template_path.name)

    render_context = dict(context)
    render_context.setdefault("request", _TemplateRequestShim(templates_dir))
    rendered_html = template.render(**render_context)

    if output_pdf_path is None:
        resolved_output_pdf = resolved_template_path.with_suffix(".pdf")
    else:
        resolved_output_pdf = Path(output_pdf_path).resolve()
    resolved_output_pdf.parent.mkdir(parents=True, exist_ok=True)

    if resolved_output_pdf.exists():
        resolved_output_pdf.unlink()

    chromium_bins = _find_chromium_binaries()
    with tempfile.TemporaryDirectory(prefix="kp_pdf_", dir=str(resolved_output_pdf.parent)) as temp_dir:
        temp_html_path = Path(temp_dir) / "rendered_kp.html"
        temp_html_path.write_text(rendered_html, encoding="utf-8")
        last_error: RuntimeError | None = None
        per_binary_errors: list[str] = []

        for chromium_bin in chromium_bins:
            try:
                _run_chromium_pdf_export(
                    chromium_bin=chromium_bin,
                    html_uri=temp_html_path.as_uri(),
                    pdf_output_path=resolved_output_pdf,
                    work_dir=Path(temp_dir),
                )
                break
            except RuntimeError as error:
                last_error = error
                per_binary_errors.append(f"{chromium_bin}: {error}")
        else:
            raise RuntimeError("Failed to export PDF via Chromium candidates. " + " || ".join(per_binary_errors)) from last_error

    if not resolved_output_pdf.exists():
        raise RuntimeError(f"PDF was not created: {resolved_output_pdf}")

    return resolved_output_pdf

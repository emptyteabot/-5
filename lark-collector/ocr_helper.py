from __future__ import annotations

import os
import shutil
from io import BytesIO
from typing import Tuple

try:
    from PIL import Image
except ImportError:  # pragma: no cover
    Image = None  # type: ignore

try:
    import pytesseract
except ImportError:  # pragma: no cover
    pytesseract = None  # type: ignore

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover
    fitz = None  # type: ignore


COMMON_TESSERACT_PATHS = [
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\Tesseract-OCR\tesseract.exe"),
    os.path.expandvars(r"%LOCALAPPDATA%\Tesseract-OCR\tesseract.exe"),
]


def _resolve_tesseract_cmd() -> str:
    env_cmd = os.getenv("TESSERACT_CMD", "").strip()
    if env_cmd and os.path.exists(env_cmd):
        return env_cmd
    which_cmd = shutil.which("tesseract")
    if which_cmd:
        return which_cmd
    for path in COMMON_TESSERACT_PATHS:
        if path and os.path.exists(path):
            return path
    return ""


def _configure_tesseract() -> str:
    if pytesseract is None:
        return ""
    cmd = _resolve_tesseract_cmd()
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd
    return cmd


def ocr_available() -> bool:
    return Image is not None and pytesseract is not None and bool(_configure_tesseract())


def ocr_image_text(data: bytes, *, lang: str = "chi_sim+eng") -> Tuple[str, bool, str]:
    if not ocr_available():
        return "", False, "pytesseract/Pillow/tesseract unavailable"
    try:
        image = Image.open(BytesIO(data))
        image = image.convert("RGB")
        text = pytesseract.image_to_string(image, lang=lang)
        return text.strip(), True, ""
    except Exception as exc:
        return "", False, str(exc)


def ocr_pdf_text(
    data: bytes,
    *,
    lang: str = "chi_sim+eng",
    max_pages: int = 8,
    scale: float = 2.0,
) -> Tuple[str, bool, str]:
    if fitz is None:
        return "", False, "PyMuPDF unavailable"
    if not ocr_available():
        return "", False, "pytesseract/Pillow/tesseract unavailable"
    try:
        doc = fitz.open(stream=data, filetype="pdf")
    except Exception as exc:
        return "", False, str(exc)

    try:
        texts: list[str] = []
        matrix = fitz.Matrix(scale, scale)
        for page in doc[: max(1, max_pages)]:
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            image = Image.open(BytesIO(pix.tobytes("png"))).convert("RGB")
            text = pytesseract.image_to_string(image, lang=lang).strip()
            if text:
                texts.append(text)
        return "\n\n".join(texts).strip(), True, ""
    except Exception as exc:
        return "", False, str(exc)
    finally:
        doc.close()

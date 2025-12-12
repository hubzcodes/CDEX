# imghdr.py
# Minimal pure-Python replacement for the stdlib imghdr.what()
# Detects common image types: jpeg, png, gif, webp, bmp
# Works by inspecting the first bytes of a file-like object or buffer.

from typing import Optional

def _read_bytes(f, n):
    try:
        pos = f.tell()
    except Exception:
        pos = None
    data = f.read(n)
    try:
        if pos is not None:
            f.seek(pos)
    except Exception:
        pass
    return data

def what(file, h: Optional[bytes] = None) -> Optional[str]:
    """
    Determine image type by header.

    - file: filename or file-like object (file.read)
    - h: optional header bytes (if provided, will be used directly)
    Returns: 'jpeg', 'png', 'gif', 'bmp', 'webp', or None
    """
    # If h provided, use that
    header = h
    handle = None

    if header is None:
        if isinstance(file, (bytes, bytearray)):
            header = bytes(file[:32])
        else:
            # try open filename string
            try:
                if isinstance(file, str):
                    with open(file, 'rb') as f:
                        header = f.read(32)
                else:
                    # assume file-like
                    handle = file
                    header = _read_bytes(handle, 32)
            except Exception:
                return None

    if not header:
        return None

    # JPEG (starts with FF D8 FF)
    if header[:3] == b'\xff\xd8\xff':
        return "jpeg"
    # PNG (89 50 4E 47 0D 0A 1A 0A)
    if header[:8] == b'\x89PNG\r\n\x1a\n':
        return "png"
    # GIF (GIF87a or GIF89a)
    if header[:6] in (b'GIF87a', b'GIF89a'):
        return "gif"
    # BMP (BM)
    if header[:2] == b'BM':
        return "bmp"
    # WEBP (RIFF .... WEBP)
    if header[:4] == b'RIFF' and header[8:12] == b'WEBP':
        return "webp"
    # TIFF (II* or MM*)
    if header[:4] in (b'II*\x00', b'MM\x00*'):
        return "tiff"
    return None


# allow import * semantics similar to stdlib
__all__ = ["what"]

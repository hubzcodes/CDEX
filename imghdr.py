# imghdr.py - small replacement for stdlib imghdr.what()
from typing import Optional

def what(file, h: Optional[bytes] = None) -> Optional[str]:
    header = h
    if header is None:
        if isinstance(file, (bytes, bytearray)):
            header = bytes(file[:32])
        else:
            try:
                if isinstance(file, str):
                    with open(file, 'rb') as f:
                        header = f.read(32)
                else:
                    header = file.read(32)
            except Exception:
                return None
    if not header:
        return None
    if header[:3] == b'\xff\xd8\xff':
        return "jpeg"
    if header[:8] == b'\x89PNG\r\n\x1a\n':
        return "png"
    if header[:6] in (b'GIF87a', b'GIF89a'):
        return "gif"
    if header[:2] == b'BM':
        return "bmp"
    if header[:4] == b'RIFF' and header[8:12] == b'WEBP':
        return "webp"
    return None

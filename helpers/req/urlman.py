from urllib.parse import urlparse
import re


def strip(url: str) -> str:
    return remove_prefix(maintain_netloc(url))


def remove_prefix(url: str) -> str:
    url = url.strip()
    if url.startswith('http'):
        url = re.sub(r'https?://', '', url, count=1)
    if url.startswith('www.'):
        url = re.sub(r'www.', '', url, count=1)
    if url.endswith('/'):
        url = url[:len(url) - 1]
    return url


def maintain_netloc(url: str) -> str:
    return str(urlparse(url).netloc)


def maintain_path(url: str) -> str:
    return str(urlparse(url).path)

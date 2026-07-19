"""Тесты курсорной пагинации — чистые функции без обращения к датасторам (задача 2.7)."""

import pytest

from app.errors import ApiError
from app.pagination import TrendingCursor, decode_cursor, encode_cursor


def test_encode_decode_roundtrip() -> None:
    cursor = TrendingCursor(stars=128, repo_id=42, rank=7)

    decoded = decode_cursor(encode_cursor(cursor))

    assert decoded == cursor


def test_encoded_cursor_is_url_safe() -> None:
    encoded = encode_cursor(TrendingCursor(stars=0, repo_id=0, rank=1))

    assert all(char not in encoded for char in "+/=")


@pytest.mark.parametrize("raw", ["not-base64!!!", "", "aGVsbG8", "W251bGwsIDQyLCAxXQ"])
def test_decode_rejects_malformed_cursor(raw: str) -> None:
    with pytest.raises(ApiError) as exc_info:
        decode_cursor(raw)

    assert exc_info.value.status_code == 400
    assert exc_info.value.code == "invalid_cursor"

"""Курсорная пагинация по keyset для `/api/v1/trending` (задача 2.7).

Курсор — непрозрачный `base64url(JSON)` над последней строкой предыдущей страницы:
`(stars, repo_id, rank)`. Keyset, а не `OFFSET`/`LIMIT`: `/trending` читает горячий агрегат
(`repo_stars_hourly_mv`, задача 2.1), который продолжает меняться между запросами двух соседних
страниц одного клиента — `OFFSET` при вставке новой лидирующей строки сдвинул бы всю пагинацию и
дал бы дубликаты или пропуски на границе страниц. Условие `stars < cursor_stars OR (stars =
cursor_stars AND repo_id > cursor_repo_id)` (см. `build_trending_query` в `app/queries.py`) в
терминах `ORDER BY stars DESC, repo_id ASC` не зависит от того, что вставилось выше текущей позиции.

`rank` в курсоре не участвует в этом условии — это просто продолжение сквозной нумерации:
без него вторая страница снова начинала бы `rank` с 1.
"""

import base64
import json
from dataclasses import dataclass

from app.errors import ApiError


@dataclass(frozen=True, slots=True)
class TrendingCursor:
    stars: int
    repo_id: int
    rank: int


def encode_cursor(cursor: TrendingCursor) -> str:
    """Сериализует курсор в непрозрачную для клиента base64url-строку без паддинга.

    Returns:
        Значение для поля `next_cursor` ответа и параметра `cursor` следующего запроса.
    """
    payload = json.dumps([cursor.stars, cursor.repo_id, cursor.rank]).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(raw: str) -> TrendingCursor:
    """Разбирает курсор из query-параметра `cursor`.

    Returns:
        Декодированный курсор.

    Raises:
        ApiError: 400, если строка не является курсором, который выдавал этот сервис (испорчена
            клиентом или подделана вручную) — клиент не должен получить 500 за чужой ввод.
    """
    try:
        padded = raw + "=" * (-len(raw) % 4)
        stars, repo_id, rank = json.loads(base64.urlsafe_b64decode(padded.encode()))
        return TrendingCursor(stars=int(stars), repo_id=int(repo_id), rank=int(rank))
    except (ValueError, TypeError) as exc:
        raise ApiError(status_code=400, code="invalid_cursor", message="Malformed cursor") from exc

#!/usr/bin/env bash
# create-topics.sh — топики шины событий. Запускается контейнером redpanda-init при каждом
# docker compose up, поэтому обязан быть идемпотентным: «топик уже существует» — это не ошибка.
#
# Почему отдельный контейнер, а не хук образа: у Redpanda нет аналога /docker-entrypoint-initdb.d,
# которым выше пользуются ClickHouse и PostgreSQL. Топики при этом обязаны существовать до первого
# продюсера — у брокера включено автосоздание (auto_create_topics_enabled=true), и оно молча
# сделало бы gh.events с одной партицией (default_topic_partitions=1) вместо шести. Пайплайн после
# этого работает, но параллельность консьюмера тихо схлопывается до единицы.
#
# Обоснование ключа, числа партиций и retention — docs/adr/0008-gh-events-topic-design.md.
set -euo pipefail

BROKERS="${KAFKA_BROKERS:-redpanda:9092}"

# ensure_topic ИМЯ ПАРТИЦИИ [k=v ...] — приводит топик к нужной форме.
# Декларативная, а не императивная: цель — конечное состояние, а не факт создания.
ensure_topic() {
  local name="$1" want="$2"
  shift 2

  local create_flags=() alter_flags=()
  local kv
  for kv in "$@"; do
    # Форма флагов у create и alter-config различается: -c против --set.
    create_flags+=(-c "$kv")
    alter_flags+=(--set "$kv")
  done

  # На существующем топике rpk вернёт ошибку — для нас это норма. Настоящий сбой (недоступный
  # брокер) поймает проверка ниже: она идёт под set -e и молча пройти не может.
  rpk topic create "$name" -X brokers="$BROKERS" -p "$want" -r 1 "${create_flags[@]}" >/dev/null 2>&1 || true

  # «Существует» не значит «правильный»: топик мог остаться от прежней конфигурации или быть создан
  # автосозданием с одной партицией. Число партиций уменьшить нельзя, а увеличивать его молча
  # означало бы разойтись с ADR — поэтому расхождение это остановка, а не тихое согласие.
  local have
  # Объявление и присваивание раздельно: local have=$(...) съел бы код возврата и обезвредил set -e.
  have="$(rpk topic list -X brokers="$BROKERS" | awk -v t="$name" '$1 == t { print $2 }')"

  if [ -z "$have" ]; then
    echo "FATAL: топик $name не создан и не найден на $BROKERS" >&2
    exit 1
  fi
  if [ "$have" != "$want" ]; then
    echo "FATAL: у топика $name партиций: $have, а требуется $want — он остался от прежней конфигурации." >&2
    echo "       Число партиций уменьшить нельзя; пересоздать окружение: docker compose down --volumes" >&2
    exit 1
  fi

  # Конфиг, в отличие от числа партиций, сходится безопасно: alter-config идемпотентен и подтягивает
  # топик, созданный на прежних значениях, под текущие.
  rpk topic alter-config "$name" -X brokers="$BROKERS" "${alter_flags[@]}" >/dev/null

  echo "OK: $name — партиций: $have"
}

# gh.events — основной поток. Ключ сообщения = event_id (ADR 0008): равномерность измерена, а не
# предположена. Шесть партиций — стартовая цель параллельности консьюмера.
#
# retention.ms=24ч и НЕ retention.bytes: лимит по размеру — единственный механизм, способный удалить
# непрочитанное при живом консьюмере, то есть молча нарушить at-least-once (ADR 0004). Без него
# режим отказа обгоняющего продюсера — остановка приёма записи, а не потеря событий.
#
# message.timestamp.type=LogAppendTime: метку ставит брокер. С CreateTime продюсер проставил бы
# время самого события, и бэкфилл 2024 года оказался бы старше retention уже в момент записи —
# то же самое, что случилось бы с TTL в ClickHouse (см. infra/clickhouse/migrations/001_events.sql).
#
# cleanup.policy=delete задаём явно: при уникальном ключе event_id компакция не удалила бы ничего
# никогда, то есть compact здесь означал бы «хранить вечно».
ensure_topic "${KAFKA_TOPIC:-gh.events}" "${KAFKA_TOPIC_PARTITIONS:-6}" \
  cleanup.policy=delete \
  retention.ms="${KAFKA_TOPIC_RETENTION_MS:-86400000}" \
  compression.type=zstd \
  message.timestamp.type=LogAppendTime

# gh.events.dlq — битые сообщения. Одна партиция: объём ничтожен, порядок не важен.
# Retention неделя, а не сутки: DLQ читает человек, а он может не заглянуть туда до понедельника.
ensure_topic "${KAFKA_DLQ_TOPIC:-gh.events.dlq}" 1 \
  cleanup.policy=delete \
  retention.ms=604800000 \
  compression.type=zstd \
  message.timestamp.type=LogAppendTime

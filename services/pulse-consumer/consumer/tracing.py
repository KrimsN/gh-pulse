"""Установка OpenTelemetry TracerProvider для pulse-consumer.

Обоснование OTLP/gRPC + Jaeger, span-links вместо parent-child на батч и того, почему это не единое
дерево span'ов от приёма события до ответа API, — docs/adr/0009-opentelemetry-tracing-jaeger.md.
"""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_tracing(service_name: str) -> TracerProvider:
    """Строит и регистрирует глобальный `TracerProvider` с OTLP/gRPC-экспортом.

    Эндпоинт коллектора берётся из стандартной переменной окружения `OTEL_EXPORTER_OTLP_ENDPOINT` —
    `OTLPSpanExporter()` без явного `endpoint` читает её сама (часть спецификации OTel SDK), отдельное
    поле в `consumer.config.Settings` под неё заводить незачем.

    Args:
        service_name: Имя сервиса в атрибуте ресурса `service.name` — по нему span'ы отличаются в
            Jaeger от `pulse-api` и `gh-collector`.

    Returns:
        Зарегистрированный `TracerProvider` — вызывающий код (`consumer.__main__`) держит ссылку
        только затем, чтобы вызвать `shutdown()` перед выходом и не потерять несброшенные span'ы.
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider

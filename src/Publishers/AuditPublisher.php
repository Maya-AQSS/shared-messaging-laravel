<?php

namespace Maya\Messaging\Publishers;

use Illuminate\Support\Facades\Log;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Jobs\RetryAmqpPublishJob;
use Throwable;

/**
 * Publishes audit events to the maya.audit exchange.
 *
 * Routing key: <application_slug>.<entity_type>.<action>
 * Queue:       audit.ingest  (#) → persisted in maya_audit.audit_log
 *
 * Failure policy — degradación ordenada:
 *   1. Si RabbitMQ rechaza el publish → se captura la excepción y se
 *      delega a Log::warning(), que intentará publicar a maya.logs
 *      (canal 'rabbit') Y escribir en el archivo diario (canal 'daily').
 *   2. Si 'rabbit' también falla (RabbitMQ caído), RabbitMQLogHandler
 *      tiene su propio try/catch y escribe el evento a stderr.
 *   3. El canal 'daily' siempre escribe en disco, independientemente
 *      del estado de RabbitMQ.
 * El resultado: un fallo en auditoría nunca bloquea la operación
 * del usuario ni lanza una excepción al controlador.
 */
class AuditPublisher
{
    public function __construct(
        private readonly MessagePublisher $publisher,
        private readonly string $exchange,
    ) {}

    public function publish(
        string $applicationSlug,
        string $entityType,
        string $entityId,
        string $action,
        string $userId,
        ?string $blockId = null,
        ?array $previousValue = null,
        ?array $newValue = null,
        ?string $ipAddress = null,
        ?string $userAgent = null,
    ): void {
        $payload = array_filter([
            'application_slug' => $applicationSlug,
            'entity_type'      => $entityType,
            'entity_id'        => $entityId,
            'block_id'         => $blockId,
            'action'           => $action,
            'user_id'          => $userId,
            'ip_address'       => $ipAddress,
            'user_agent'       => $userAgent,
            'previous_value'   => $previousValue,
            'new_value'        => $newValue,
            'occurred_at'      => $this->occurredAtForPayload(),
        ], fn ($v) => $v !== null);

        try {
            $this->publisher->publish(
                exchange: $this->exchange,
                routingKey: "{$applicationSlug}.{$entityType}.{$action}",
                payload: $payload,
            );
        } catch (Throwable $e) {
            Log::warning('audit.publish_failed', [
                'exchange'         => $this->exchange,
                'application_slug' => $applicationSlug,
                'entity_type'      => $entityType,
                'entity_id'        => $entityId,
                'action'           => $action,
                'user_id'          => $userId,
                'error'            => $e->getMessage(),
            ]);

            try {
                RetryAmqpPublishJob::dispatch(
                    exchange: $this->exchange,
                    routingKey: "{$applicationSlug}.{$entityType}.{$action}",
                    payload: $payload,
                );
            } catch (Throwable $dbError) {
                Log::error('amqp.queue_fallback_failed', [
                    'exchange'    => $this->exchange,
                    'routing_key' => "{$applicationSlug}.{$entityType}.{$action}",
                    'error'       => $dbError->getMessage(),
                ]);
            }
        }
    }

    /**
     * Marca temporal del evento. Si existe {@see config('messaging.audit_timestamp_timezone')}
     * (IANA, p. ej. Europe/Madrid), se emite ISO 8601 con offset local para alinear con UIs que
     * muestran payloads sin convertir UTC; si no, UTC con sufijo Z (comportamiento anterior).
     */
    private function occurredAtForPayload(): string
    {
        $tz = config('messaging.audit_timestamp_timezone');
        if (is_string($tz) && $tz !== '') {
            try {
                return now()->timezone($tz)->toIso8601String();
            } catch (Throwable) {
                // Zona IANA inválida: degradar a UTC.
            }
        }

        return now()->utc()->toIso8601ZuluString();
    }
}

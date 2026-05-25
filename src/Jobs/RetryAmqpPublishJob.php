<?php

namespace Maya\Messaging\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Maya\Messaging\Contracts\MessagePublisher;
use Throwable;

/**
 * Reintenta un publish AMQP fallido usando la cola de base de datos como
 * buffer cuando RabbitMQ no está disponible.
 *
 * Usado por AuditPublisher, NotificationPublisher y AlertPublisher.
 * LogPublisher queda excluido: sus mensajes ya van a disco vía canal
 * 'daily', el volumen es alto, y reintentar logs retrasados tiene poco
 * valor operativo.
 *
 * Siempre despacha sobre connection='database' para funcionar aunque
 * QUEUE_CONNECTION apunte a rabbitmq (ej. maya_dms).
 *
 * Backoff exponencial: 30s → 60s → 120s → 300s → 600s (~17 min total).
 */
class RetryAmqpPublishJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 5;

    public array $backoff = [30, 60, 120, 300, 600];

    public function __construct(
        private readonly string $exchange,
        private readonly string $routingKey,
        private readonly array $payload,
        private readonly array $properties = [],
    ) {
        $this->connection = 'database';
    }

    public function handle(MessagePublisher $publisher): void
    {
        $publisher->publish(
            exchange: $this->exchange,
            routingKey: $this->routingKey,
            payload: $this->payload,
            properties: $this->properties,
        );
    }

    public function failed(Throwable $exception): void
    {
        Log::error('amqp.retry_exhausted', [
            'exchange'    => $this->exchange,
            'routing_key' => $this->routingKey,
            'tries'       => $this->tries,
            'error'       => $exception->getMessage(),
        ]);
    }
}

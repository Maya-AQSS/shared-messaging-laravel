<?php

namespace Maya\Messaging\Publishers;

use Illuminate\Support\Facades\Log;
use InvalidArgumentException;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Jobs\RetryAmqpPublishJob;
use Ramsey\Uuid\Uuid;
use Throwable;

/**
 * Publishes user-facing notifications.
 *
 * One AMQP publish per requested channel, so RabbitMQ routes each copy to the
 * correct downstream queue (notifications.email → n8n SMTP, etc). Every copy
 * carries the same `message_id`, so the ingest consumer dedupes and persists
 * the notification once regardless of how many channels were requested.
 *
 *   Routing key:  <app>.<type>.<channel>
 *   Bindings:     notifications.ingest  (*.*.*)   — always (dedup by message_id)
 *                 notifications.email   (*.*.email) — SMTP fan-out (n8n)
 */
class NotificationPublisher
{
    private const VALID_CHANNELS = ['app', 'email', 'webhook', 'slack'];

    public function __construct(
        private readonly MessagePublisher $publisher,
        private readonly string $exchange,
        private readonly string $defaultApp,
    ) {}

    /**
     * @param string[] $channels  one or more of VALID_CHANNELS
     */
    public function send(
        string $type,
        string $recipientId,
        string $title,
        string $body,
        array $channels = ['app'],
        array $metadata = [],
        ?string $app = null,
        ?string $createdAt = null,
    ): void {
        $app = $app ?? $this->defaultApp;
        $channels = array_values(array_unique($channels));

        if ($channels === []) {
            throw new InvalidArgumentException('At least one notification channel is required.');
        }

        $validChannels = config('messaging.notifications.valid_channels', self::VALID_CHANNELS);
        foreach ($channels as $channel) {
            if (!in_array($channel, $validChannels, true)) {
                throw new InvalidArgumentException("Invalid notification channel: {$channel}");
            }
        }

        $payload = [
            'app'                   => $app,
            'type'                  => $type,
            'recipient_keycloak_id' => $recipientId,
            'title'                 => $title,
            'body'         => $body,
            'channels'     => $channels,
            'metadata'     => (object) $metadata,
            'created_at'   => $createdAt ?? now()->toIso8601ZuluString(),
        ];

        $sharedMessageId = Uuid::uuid4()->toString();

        foreach ($channels as $channel) {
            try {
                $this->publisher->publish(
                    exchange: $this->exchange,
                    routingKey: "{$app}.{$type}.{$channel}",
                    payload: $payload,
                    properties: ['message_id' => $sharedMessageId],
                );
            } catch (Throwable $e) {
                Log::warning('notification.publish_failed', [
                    'exchange' => $this->exchange,
                    'app'      => $app,
                    'type'     => $type,
                    'channel'  => $channel,
                    'error'    => $e->getMessage(),
                ]);

                try {
                    RetryAmqpPublishJob::dispatch(
                        exchange: $this->exchange,
                        routingKey: "{$app}.{$type}.{$channel}",
                        payload: $payload,
                        properties: ['message_id' => $sharedMessageId],
                    );
                } catch (Throwable $dbError) {
                    Log::error('amqp.queue_fallback_failed', [
                        'exchange'    => $this->exchange,
                        'routing_key' => "{$app}.{$type}.{$channel}",
                        'error'       => $dbError->getMessage(),
                    ]);
                }
            }
        }
    }
}

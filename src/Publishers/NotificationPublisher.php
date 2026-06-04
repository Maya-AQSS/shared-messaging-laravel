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

    private const VALID_SEVERITIES = ['critical', 'high', 'medium', 'low', 'info'];

    public function __construct(
        private readonly MessagePublisher $publisher,
        private readonly string $exchange,
        private readonly string $defaultApp,
    ) {}

    /**
     * @param  string[]  $channels  one or more of VALID_CHANNELS
     * @param  array<string, mixed>  $metadata
     * @param  array<string, mixed>  $params  interpolation params for the i18n keys
     *
     * Content is either free text ($title/$body — e.g. manual alerts) or i18n
     * keys ($titleKey/$bodyKey + $params) resolved per recipient locale at the
     * dashboard. Prefer keys for system notifications so each user reads the
     * message in their own language.
     */
    public function send(
        string $type,
        ?string $recipientId = null,
        string $title = '',
        string $body = '',
        array $channels = ['app'],
        array $metadata = [],
        ?string $app = null,
        ?string $createdAt = null,
        bool $isCritical = false,
        string $scope = 'user',
        ?string $severity = null,
        ?string $url = null,
        ?string $titleKey = null,
        ?string $bodyKey = null,
        array $params = [],
    ): void {
        $app = $app ?? $this->defaultApp;
        $channels = array_values(array_unique($channels));

        // Validate scope
        if (!in_array($scope, ['user', 'dashboard', 'both'], true)) {
            throw new InvalidArgumentException("Invalid notification scope: {$scope}");
        }

        // Validate recipientId requirement
        if (($scope === 'user' || $scope === 'both') && $recipientId === null) {
            throw new InvalidArgumentException("recipientId is required when scope is 'user' or 'both'");
        }

        if ($channels === []) {
            throw new InvalidArgumentException('At least one notification channel is required.');
        }

        $validChannels = config('messaging.notifications.valid_channels', self::VALID_CHANNELS);
        foreach ($channels as $channel) {
            if (!in_array($channel, $validChannels, true)) {
                throw new InvalidArgumentException("Invalid notification channel: {$channel}");
            }
        }

        // Reconcile severity ⇄ is_critical. Severity is authoritative when given;
        // otherwise derive it from the legacy boolean for forward-compatibility.
        if ($severity !== null && !in_array($severity, self::VALID_SEVERITIES, true)) {
            throw new InvalidArgumentException("Invalid notification severity: {$severity}");
        }

        $severity ??= $isCritical ? 'high' : 'info';
        $isCritical = in_array($severity, ['critical', 'high'], true);

        $payload = [
            'app'                   => $app,
            'type'                  => $type,
            'recipient_keycloak_id' => $recipientId ?? '',
            'title'                 => $title,
            'body'         => $body,
            'title_key'    => $titleKey,
            'body_key'     => $bodyKey,
            'params'       => (object) $params,
            'severity'     => $severity,
            'url'          => $url,
            'channels'     => $channels,
            'metadata'     => (object) $metadata,
            'created_at'   => $createdAt ?? now()->toIso8601ZuluString(),
            'is_critical'  => $isCritical,
            'scope'        => $scope,
        ];

        $sharedMessageId = Uuid::uuid4()->toString();

        foreach ($channels as $channel) {
            $routingKey = $isCritical
                ? "{$app}.{$type}.{$channel}.critical"
                : "{$app}.{$type}.{$channel}";

            try {
                $this->publisher->publish(
                    exchange: $this->exchange,
                    routingKey: $routingKey,
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
                        routingKey: $routingKey,
                        payload: $payload,
                        properties: ['message_id' => $sharedMessageId],
                    );
                } catch (Throwable $dbError) {
                    Log::error('amqp.queue_fallback_failed', [
                        'exchange'    => $this->exchange,
                        'routing_key' => $routingKey,
                        'error'       => $dbError->getMessage(),
                    ]);
                }
            }
        }
    }
}

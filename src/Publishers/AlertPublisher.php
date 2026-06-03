<?php

namespace Maya\Messaging\Publishers;

use Illuminate\Support\Facades\Log;
use InvalidArgumentException;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Jobs\RetryAmqpPublishJob;
use Throwable;

/**
 * Publishes alerts. Routing key <rule_slug>.<severity>.
 * Bindings:  alerts.ingest  (#)  → persisted in maya_dashboard.alerts
 *            alerts.dispatch (#) → n8n webhook / Slack fan-out.
 */
class AlertPublisher
{
    private const VALID_SEVERITIES = ['critical', 'high', 'medium', 'low'];
    private const VALID_SOURCES = ['logs.aggregation', 'metric.threshold', 'app.publish', 'manual', 'system.dlq'];

    public function __construct(
        private readonly MessagePublisher $publisher,
        private readonly string $exchange,
        private readonly NotificationPublisher $notificationPublisher,
    ) {}

    /**
     * @param  list<string>  $recipientIds  Destinatarios Keycloak cuando notifyAll=false
     */
    public function publish(
        string $ruleSlug,
        string $severity,
        string $title,
        array $context = [],
        string $source = 'app.publish',
        ?string $createdAt = null,
        bool $notifyAll = true,
        array $recipientIds = [],
    ): void {
        if (!in_array($severity, self::VALID_SEVERITIES, true)) {
            throw new InvalidArgumentException("Invalid alert severity: {$severity}");
        }
        if (!in_array($source, self::VALID_SOURCES, true)) {
            throw new InvalidArgumentException("Invalid alert source: {$source}");
        }

        $payload = [
            'rule_slug'  => $ruleSlug,
            'severity'   => $severity,
            'title'      => $title,
            'source'     => $source,
            'context'    => (object) $context,
            'created_at' => $createdAt ?? now()->toIso8601ZuluString(),
        ];

        try {
            $this->publisher->publish(
                exchange: $this->exchange,
                routingKey: "{$ruleSlug}.{$severity}",
                payload: $payload,
            );
        } catch (Throwable $e) {
            Log::warning('alert.publish_failed', [
                'exchange'  => $this->exchange,
                'rule_slug' => $ruleSlug,
                'severity'  => $severity,
                'error'     => $e->getMessage(),
            ]);

            try {
                RetryAmqpPublishJob::dispatch(
                    exchange: $this->exchange,
                    routingKey: "{$ruleSlug}.{$severity}",
                    payload: $payload,
                );
            } catch (Throwable $dbError) {
                Log::error('amqp.queue_fallback_failed', [
                    'exchange'    => $this->exchange,
                    'routing_key' => "{$ruleSlug}.{$severity}",
                    'error'       => $dbError->getMessage(),
                ]);
            }
        }

        $isCritical = in_array($severity, ['critical', 'high'], true);
        $notificationMetadata = [
            'severity' => $severity,
            'source' => $source,
            'rule_slug' => $ruleSlug,
            'context' => $context,
        ];

        try {
            if ($notifyAll) {
                // Alerta global: scope dashboard (sin destinatario concreto).
                $this->notificationPublisher->send(
                    type: "alert.{$ruleSlug}",
                    recipientId: null,
                    title: $title,
                    body: "Severidad: {$severity}. Origen: {$source}.",
                    channels: ['app'],
                    metadata: $notificationMetadata,
                    app: null,
                    createdAt: $createdAt,
                    isCritical: $isCritical,
                    scope: 'dashboard',
                );
            } else {
                foreach ($recipientIds as $recipientId) {
                    $this->notificationPublisher->send(
                        type: "alert.{$ruleSlug}",
                        recipientId: $recipientId,
                        title: $title,
                        body: "Severidad: {$severity}. Origen: {$source}.",
                        channels: ['app'],
                        metadata: $notificationMetadata,
                        app: null,
                        createdAt: $createdAt,
                        isCritical: $isCritical,
                        scope: 'user',
                    );
                }
            }
        } catch (Throwable $e) {
            Log::warning('alert.notification_publish_failed', [
                'rule_slug' => $ruleSlug,
                'severity' => $severity,
                'error' => $e->getMessage(),
            ]);
        }
    }
}

<?php

namespace Maya\Messaging\Publishers;

use Illuminate\Support\Facades\Log;
use Maya\Messaging\Contracts\MessagePublisher;
use Throwable;

/**
 * High-level publisher for log events.
 * Routing key: <app>.<severity>  → bound to queue logs.ingest.
 *
 * Severity must be one of: critical | high | medium | low | other.
 * Anything unrecognized is mapped to "other".
 *
 * Log events are intentionally NOT retried via RetryAmqpPublishJob: volume is high,
 * retrying delayed logs has low operational value, and they are already persisted to
 * disk via the 'daily' channel.
 */
class LogPublisher
{
    private const VALID_SEVERITIES = ['critical', 'high', 'medium', 'low', 'other'];

    public function __construct(
        private readonly MessagePublisher $publisher,
        private readonly string $exchange,
        private readonly string $defaultApp,
    ) {}

    public function publish(
        string $severity,
        string $message,
        ?string $errorCode = null,
        ?string $file = null,
        ?int $line = null,
        array $metadata = [],
        ?string $app = null,
        ?string $occurredAt = null,
    ): void {
        $app = $app ?? $this->defaultApp;
        $severity = in_array($severity, self::VALID_SEVERITIES, true) ? $severity : 'other';

        $payload = [
            'app'         => $app,
            'severity'    => $severity,
            'message'     => $message,
            'error_code'  => $errorCode,
            'file'        => $file,
            'line'        => $line,
            'metadata'    => (object) $metadata,
            'occurred_at' => $occurredAt ?? now()->toIso8601ZuluString(),
        ];

        try {
            $this->publisher->publish(
                exchange: $this->exchange,
                routingKey: "{$app}.{$severity}",
                payload: $payload,
            );
        } catch (Throwable $e) {
            Log::warning('log.publish_failed', [
                'exchange'  => $this->exchange,
                'app'       => $app,
                'severity'  => $severity,
                'error'     => $e->getMessage(),
            ]);
        }
    }
}

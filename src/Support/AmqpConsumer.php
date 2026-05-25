<?php

namespace Maya\Messaging\Support;

use Closure;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Throwable;

/**
 * Reusable AMQP consumer loop.
 *
 * Handles boilerplate for every Maya consumer command:
 *   - idempotency dedup by message_id (Redis cache, 24h TTL)
 *   - JSON decoding + schema-agnostic delivery to the user callback
 *   - ACK on success, NACK(requeue=true) up to maxRetries, then NACK(requeue=false) → DLQ
 *   - graceful shutdown on SIGTERM/SIGINT
 */
class AmqpConsumer
{
    private bool $stopRequested = false;

    public function __construct(
        private readonly AmqpConnectionFactory $factory,
        private readonly int $prefetch = 50,
        private readonly int $maxRetries = 3,
        private readonly int $dedupTtlSeconds = 86400,
    ) {}

    /**
     * @param Closure(array $payload, AMQPMessage $raw): void $handler
     *        Should throw on failure; return for success.
     */
    public function consume(string $queue, Closure $handler, ?Closure $shouldStop = null): void
    {
        $channel = $this->factory->connection()->channel();
        $channel->basic_qos(null, $this->prefetch, null);

        $callback = function (AMQPMessage $message) use ($handler): void {
            $messageId = $message->has('message_id') ? $message->get('message_id') : null;

            if ($messageId !== null && $this->alreadyProcessed($messageId)) {
                $message->ack();
                return;
            }

            try {
                $payload = json_decode($message->getBody(), associative: true, depth: 32, flags: JSON_THROW_ON_ERROR);
            } catch (Throwable $e) {
                Log::channel('single')->error('Invalid JSON in AMQP payload, sending to DLQ', [
                    'message_id' => $messageId,
                    'body_preview' => mb_substr($message->getBody(), 0, 256),
                    'error' => $e->getMessage(),
                ]);
                $message->nack(false);
                return;
            }

            try {
                $handler($payload, $message);
                if ($messageId !== null) {
                    $this->markProcessed($messageId);
                }
                $message->ack();
            } catch (Throwable $e) {
                $this->handleFailure($message, $messageId, $e);
            }
        };

        $channel->basic_consume(
            queue: $queue,
            consumer_tag: '',
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            callback: $callback,
        );

        $this->installSignalHandlers();

        while ($channel->is_consuming()) {
            if ($this->stopRequested || ($shouldStop !== null && $shouldStop())) {
                break;
            }
            try {
                $channel->wait(null, false, 5);
            } catch (AMQPTimeoutException $e) {
                // No message arrived within the 5s window — that's normal,
                // continue the loop so signal handlers and shouldStop get
                // a chance to run between waits.
                continue;
            } catch (AMQPRuntimeException $e) {
                Log::channel('single')->warning('AMQP wait interrupted, reconnecting', [
                    'queue' => $queue,
                    'error' => $e->getMessage(),
                ]);
                break;
            }
        }

        $channel->close();
    }

    private function alreadyProcessed(string $messageId): bool
    {
        return (bool) Cache::has($this->dedupKey($messageId));
    }

    private function markProcessed(string $messageId): void
    {
        Cache::put($this->dedupKey($messageId), 1, $this->dedupTtlSeconds);
    }

    private function dedupKey(string $messageId): string
    {
        return "messaging:seen:{$messageId}";
    }

    private function handleFailure(AMQPMessage $message, ?string $messageId, Throwable $e): void
    {
        $headers = $message->has('application_headers') ? $message->get('application_headers')->getNativeData() : [];
        $deathCount = 0;
        foreach ($headers['x-death'] ?? [] as $death) {
            $deathCount += (int) ($death['count'] ?? 0);
        }

        Log::channel('single')->warning('Consumer handler failed', [
            'message_id' => $messageId,
            'retries' => $deathCount,
            'error' => $e->getMessage(),
        ]);

        if ($deathCount >= $this->maxRetries) {
            // Exhausted retries → send to DLQ.
            $message->nack(false);
        } else {
            $message->nack(true);
        }
    }

    private function installSignalHandlers(): void
    {
        if (!function_exists('pcntl_signal')) {
            return;
        }
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, function () { $this->stopRequested = true; });
        pcntl_signal(SIGINT, function () { $this->stopRequested = true; });
    }
}

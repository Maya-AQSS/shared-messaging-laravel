<?php

declare(strict_types=1);

namespace Maya\Messaging\Console;

use Illuminate\Console\Command;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Log;
use Maya\Messaging\Exceptions\UnrecoverableIngestionException;
use Maya\Messaging\Support\AmqpConsumer;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Abstract base command for Maya AMQP consumers.
 *
 * Encapsulates the canonical error-classification policy used across all
 * Maya consumer workers (maya_audit, maya_logs, maya_dashboard), derived
 * from maya_logs/ConsumeLogs — the most robust implementation:
 *
 *   - {@see UnrecoverableIngestionException} → ACK/drop (no retry).
 *     Payload is malformed or semantically invalid; retrying would be
 *     pointless and would block the queue.
 *
 *   - {@see QueryException} → rethrow → NACK/retry.
 *     Infrastructure failure; broker retries after the configured delay.
 *
 *   - Any other {@see \Throwable} → log error + report() + ACK/drop.
 *     Unexpected failure; logged and dropped to avoid permanent queue blockage.
 *
 * Subclasses must implement:
 *   - {@see queueName()}: return the AMQP queue to consume from.
 *   - {@see ingest()}: process a single decoded payload.
 *
 * Subclasses may override:
 *   - {@see flush()}: called after the consume loop exits to drain any
 *     internal write buffer (e.g. batch inserts). Default is a no-op.
 *
 * Usage in subclasses:
 *
 *     class ConsumeAudit extends ConsumeQueueCommand
 *     {
 *         protected $signature = 'audit:consume {--queue=audit.ingest}';
 *         protected $description = 'Consume audit events from RabbitMQ';
 *
 *         public function queueName(): string
 *         {
 *             return (string) ($this->option('queue') ?: config('messaging.queues.audit_ingest', 'audit.ingest'));
 *         }
 *
 *         public function ingest(array $payload, AMQPMessage $message): void
 *         {
 *             $this->ingestionService->ingest($payload);
 *         }
 *     }
 */
abstract class ConsumeQueueCommand extends Command
{
    /**
     * The AMQP queue name to consume from.
     * Typically derived from a CLI option or config value.
     */
    abstract public function queueName(): string;

    /**
     * Process a single decoded AMQP payload.
     *
     * Throwing {@see UnrecoverableIngestionException} signals a bad payload
     * (will be ACKed/dropped). Throwing {@see QueryException} signals an
     * infrastructure failure (will be NACKed/retried). Any other exception
     * is treated as unexpected and causes an ACK/drop with error logging.
     *
     * @param  array<string, mixed>  $payload  Decoded JSON payload.
     * @param  AMQPMessage           $message  Raw AMQP message (for headers, routing key, etc.).
     *
     * @throws UnrecoverableIngestionException  to trigger ACK/drop.
     * @throws QueryException                   to trigger NACK/retry.
     */
    abstract public function ingest(array $payload, AMQPMessage $message): void;

    /**
     * Drain any internal write buffer after the consume loop exits.
     *
     * Override in subclasses that buffer writes for batch inserts
     * (e.g. LogIngestionService). Default is a no-op.
     */
    public function flush(): void {}

    /**
     * Run the consumer loop.
     * Injected AmqpConsumer handles JSON decode, dedup, ACK/NACK, and SIGTERM.
     */
    public function handle(AmqpConsumer $consumer): int
    {
        $queue = $this->queueName();
        $this->info("Consuming from queue: {$queue}");

        $consumer->consume($queue, function (array $payload, AMQPMessage $message): void {
            try {
                $this->ingest($payload, $message);
            } catch (UnrecoverableIngestionException $e) {
                // Validation / business-rule failure → drop so the queue stays unblocked.
                // AmqpConsumer ACKs after the handler returns normally.
                Log::warning(static::class . ': dropping unrecoverable message', [
                    'error'        => $e->getMessage(),
                    'payload_keys' => array_keys($payload),
                ]);
            } catch (QueryException $e) {
                // Infrastructure failure → rethrow so AmqpConsumer NACKs and retries.
                Log::error(static::class . ': infrastructure error, will nack for retry', [
                    'error' => $e->getMessage(),
                ]);
                throw $e;
            } catch (\Throwable $e) {
                // Unexpected error → log, report, and drop to avoid queue blockage.
                Log::error(static::class . ': unexpected error, dropping message', [
                    'error'        => $e->getMessage(),
                    'payload_keys' => array_keys($payload),
                ]);
                report($e);
            }
        });

        // flush() corre fuera del catch por-mensaje del loop: si el drenado del
        // buffer falla (p.ej. QueryException en el insert batch) no debe tumbar
        // el worker en silencio — se loguea, se reporta y se sale con FAILURE.
        try {
            $this->flush();
        } catch (\Throwable $e) {
            Log::error(static::class.': flush() falló tras el consume loop', [
                'error' => $e->getMessage(),
            ]);
            report($e);

            return self::FAILURE;
        }

        return self::SUCCESS;
    }
}

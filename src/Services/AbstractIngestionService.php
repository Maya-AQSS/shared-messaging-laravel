<?php

declare(strict_types=1);

namespace Maya\Messaging\Services;

use Illuminate\Support\Facades\Log;
use Maya\Messaging\Contracts\IngestionService;

/**
 * Template method base class for AMQP ingestion services.
 *
 * Implements the canonical try-parse → catch InvalidArgumentException →
 * log/drop → persist pattern used across Maya consumers (maya_audit,
 * maya_logs). Subclasses implement parse() and persist(); the ingest()
 * orchestration is not overrideable.
 *
 * Error contract (mirrors ConsumeLogs/ConsumeAudit conventions):
 *
 *  - parse() throws {@see \InvalidArgumentException}:
 *      Payload is invalid (missing fields, wrong types).  The message is
 *      logged and silently dropped — ingest() returns normally so the
 *      caller ACKs.
 *
 *  - parse() throws {@see \Maya\Messaging\Exceptions\UnrecoverableIngestionException}:
 *      Semantically invalid (unknown app slug, business-rule violation).
 *      Re-thrown so the command-level handler can ACK/drop explicitly with
 *      its own log message.
 *
 *  - persist() throws any {@see \Throwable}:
 *      Infrastructure failure (DB, connection).  Re-thrown so the consumer
 *      can NACK and retry.
 */
abstract class AbstractIngestionService implements IngestionService
{
    /**
     * Parse a raw AMQP payload into a typed DTO.
     *
     * @param  array<string, mixed>  $payload
     * @return object  A typed DTO ready for persistence.
     *
     * @throws \InvalidArgumentException        if required fields are missing or invalid.
     * @throws \Maya\Messaging\Exceptions\UnrecoverableIngestionException
     *                                          if the payload is semantically unprocessable.
     */
    abstract protected function parse(array $payload): object;

    /**
     * Persist the parsed DTO to storage.
     *
     * @throws \Throwable on infrastructure failure.
     */
    abstract protected function persist(object $dto): void;

    /**
     * Ingest a raw AMQP payload.
     *
     * Orchestrates parse → drop-on-invalid → persist.
     */
    final public function ingest(array $payload): void
    {
        try {
            $dto = $this->parse($payload);
        } catch (\InvalidArgumentException $e) {
            Log::warning(static::class . ': payload descartado por campos inválidos', [
                'reason'       => $e->getMessage(),
                'payload_keys' => array_keys($payload),
            ]);

            return;
        }

        // Persist may throw — let it propagate for NACK/retry.
        $this->persist($dto);
    }
}

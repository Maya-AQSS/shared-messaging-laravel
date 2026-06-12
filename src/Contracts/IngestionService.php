<?php

declare(strict_types=1);

namespace Maya\Messaging\Contracts;

/**
 * Contract for services that ingest a raw AMQP payload into persistent storage.
 *
 * Implementations are expected to:
 *  - Parse/validate the payload (throwing on unrecoverable errors so the consumer can ACK/drop).
 *  - Persist the parsed data (throwing on infrastructure failures so the consumer can NACK/retry).
 */
interface IngestionService
{
    /**
     * Ingest a raw AMQP payload.
     *
     * @param  array<string, mixed>  $payload  Decoded JSON payload from the AMQP message.
     *
     * @throws \Maya\Messaging\Exceptions\UnrecoverableIngestionException
     *         if the payload is fundamentally malformed and must be dropped.
     * @throws \Throwable for infrastructure errors that should trigger a NACK/retry.
     */
    public function ingest(array $payload): void;
}

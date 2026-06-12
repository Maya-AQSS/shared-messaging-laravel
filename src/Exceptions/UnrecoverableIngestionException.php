<?php

declare(strict_types=1);

namespace Maya\Messaging\Exceptions;

use RuntimeException;

/**
 * Thrown by an ingestion service when a message cannot be retried:
 * unknown application slug, malformed payload, or other business-rule
 * violations that make retrying pointless.
 *
 * Consumers catching this exception should ACK (drop) the message to
 * avoid permanently blocking the queue. This mirrors the app-level
 * `App\Exceptions\UnrecoverableIngestionException` in maya_logs, lifted
 * here so all consumers can share the type without depending on each
 * other's application namespace.
 */
final class UnrecoverableIngestionException extends RuntimeException {}

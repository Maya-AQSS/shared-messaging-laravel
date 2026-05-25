<?php

namespace Maya\Messaging\Logging;

use Maya\Messaging\Publishers\LogPublisher;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;
use Throwable;

/**
 * Monolog handler that forwards every log record to the maya.logs exchange.
 *
 * Wired via config/logging.php as a custom driver. The level mapping is
 * defined in config/messaging.php (logging.map_levels) — PSR log levels
 * collapse into the 5 severities of the maya.logs contract.
 */
class RabbitMQLogHandler extends AbstractProcessingHandler
{
    /**
     * @param array<string,string> $levelMap PSR level name → maya severity
     */
    public function __construct(
        private readonly LogPublisher $publisher,
        private readonly array $levelMap,
        int|string|Level $level = Level::Warning,
        bool $bubble = true,
    ) {
        parent::__construct($level, $bubble);
    }

    protected function write(LogRecord $record): void
    {
        $psrLevel = strtolower($record->level->getName());
        $severity = $this->levelMap[$psrLevel] ?? 'other';

        $context = $record->context;
        $exception = $context['exception'] ?? null;
        unset($context['exception']);

        $file = null;
        $line = null;
        $errorCode = $context['error_code'] ?? null;
        unset($context['error_code']);

        if ($exception instanceof Throwable) {
            $file = $exception->getFile();
            $line = $exception->getLine();
            $context['trace'] = $this->shortTrace($exception);
            $errorCode = $errorCode ?? get_class($exception);
        }

        try {
            $this->publisher->publish(
                severity: $severity,
                message: $record->message,
                errorCode: $errorCode,
                file: $file,
                line: $line,
                metadata: $context,
                occurredAt: $record->datetime->format('Y-m-d\\TH:i:s\\Z'),
            );
        } catch (Throwable $e) {
            // Never let logging break the app. Fall back to stderr so the
            // container logs still show the failure.
            fwrite(STDERR, sprintf(
                "[rabbitmq-log-handler] publish failed: %s — original message: %s\n",
                $e->getMessage(),
                $record->message,
            ));
        }
    }

    private function shortTrace(Throwable $exception): string
    {
        $lines = explode("\n", $exception->getTraceAsString());
        return implode("\n", array_slice($lines, 0, 12));
    }
}

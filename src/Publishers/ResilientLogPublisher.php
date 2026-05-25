<?php

declare(strict_types=1);

namespace Maya\Messaging\Publishers;

use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Envuelve {@see LogPublisher} para publicar fallos a `maya.logs` sin que
 * un error del broker enmascare la excepción original del flujo de negocio.
 *
 * Reutilizable desde cualquier servicio que quiera registrar un fallo
 * estructurado y seguir relanzando o devolviendo la excepción original.
 * Si la publicación AMQP falla, se loggea localmente vía `Log::warning`
 * con la clave estable `maya.logs.publish_failed_after_operation_failure`
 * (útil para monitoring + alertas) y NO se rethrowa.
 */
final class ResilientLogPublisher
{
    public function __construct(
        private readonly LogPublisher $logPublisher,
    ) {}

    /**
     * Publica un log estructurado derivado de un `Throwable`. Usa `getFile()`
     * y `getLine()` automáticamente.
     *
     * @param  array<string, mixed>  $metadata
     */
    public function publishFromThrowable(
        Throwable $original,
        string $severity,
        string $errorCode,
        array $metadata,
        string $app,
    ): void {
        try {
            $this->logPublisher->publish(
                severity: $severity,
                message: $original->getMessage(),
                errorCode: $errorCode,
                file: $original->getFile(),
                line: $original->getLine(),
                metadata: $metadata,
                app: $app,
            );
        } catch (Throwable $publishError) {
            Log::warning('maya.logs.publish_failed_after_operation_failure', [
                'app' => $app,
                'error_code' => $errorCode,
                'original_class' => $original::class,
                'original_message' => $original->getMessage(),
                'publish_error_class' => $publishError::class,
                'publish_error' => $publishError->getMessage(),
            ]);
        }
    }

    /**
     * Publica un log estructurado sin Throwable de origen — útil para
     * fallos detectados manualmente (validación, business rule violation).
     *
     * @param  array<string, mixed>  $metadata
     */
    public function publishStructured(
        string $severity,
        string $message,
        string $errorCode,
        array $metadata,
        string $app,
        ?string $file = null,
        ?int $line = null,
    ): void {
        try {
            $this->logPublisher->publish(
                severity: $severity,
                message: $message,
                errorCode: $errorCode,
                file: $file,
                line: $line,
                metadata: $metadata,
                app: $app,
            );
        } catch (Throwable $publishError) {
            Log::warning('maya.logs.publish_failed_after_operation_failure', [
                'app' => $app,
                'error_code' => $errorCode,
                'original_message' => $message,
                'publish_error_class' => $publishError::class,
                'publish_error' => $publishError->getMessage(),
            ]);
        }
    }
}

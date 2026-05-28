<?php

namespace Maya\Messaging\Consumers;

use Closure;
use Illuminate\Support\Facades\Log;
use Illuminate\Validation\ValidationException;
use Maya\Messaging\Support\AmqpConsumer;
use Maya\Messaging\Support\AmqpConnectionFactory;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use Throwable;

/**
 * Wrapper sobre AmqpConsumer que añade clasificación de errores por mensaje.
 *
 * Distingue dos categorías de fallo al procesar un mensaje:
 *
 *  - **Errores de payload** (`ValidationException`, errores de parsing/JSON):
 *    El mensaje es inválido y reintentar no tiene sentido. Se hace drop (ack)
 *    y se llama al callback `onPoisonMessage` para logging personalizado.
 *    Estos errores NO cuentan para el contador de errores consecutivos.
 *
 *  - **Errores de infraestructura** (DB, conexiones, excepciones no esperadas):
 *    Se re-lanza la excepción para que `AmqpConsumer` aplique su lógica de
 *    nack/retry normal. Incrementan el contador de errores consecutivos.
 *
 * Si el número de errores consecutivos de infraestructura supera
 * `maxConsecutiveErrors`, el consumer se detiene limpiamente (el bucle de
 * `AmqpConsumer` evalúa `shouldStop` y sale) y se lanza una `RuntimeException`
 * al caller para que pueda alertar/reiniciar el proceso.
 *
 * Uso típico:
 *
 *     $safe = new SafeAmqpConsumer(
 *         factory: $factory,
 *         onPoisonMessage: fn ($payload, $e, $raw) => Log::warning('poison', ['error' => $e->getMessage()]),
 *     );
 *     $safe->consume('maya.audit.created', function (array $payload, AMQPMessage $raw) {
 *         // procesar el mensaje
 *     });
 */
class SafeAmqpConsumer
{
    private int $consecutiveErrors = 0;
    private bool $shouldHalt = false;
    private ?RuntimeException $haltReason = null;

    public function __construct(
        private readonly AmqpConnectionFactory $factory,
        private readonly int $prefetch = 50,
        private readonly int $maxRetries = 3,
        private readonly int $dedupTtlSeconds = 86400,
        private readonly int $maxConsecutiveErrors = 10,
        private readonly ?Closure $onPoisonMessage = null,
    ) {}

    /**
     * Consume mensajes de la cola indicada aplicando clasificación de errores.
     *
     * @param  Closure(array $payload, AMQPMessage $raw): void  $handler
     *         Lanzar excepción en caso de error; retornar para señalar éxito.
     * @throws RuntimeException cuando se supera `maxConsecutiveErrors`
     */
    public function consume(string $queue, Closure $handler, ?Closure $shouldStop = null): void
    {
        // Reset state on each invocation so the instance can be reused.
        $this->consecutiveErrors = 0;
        $this->shouldHalt = false;
        $this->haltReason = null;

        $inner = new AmqpConsumer(
            factory: $this->factory,
            prefetch: $this->prefetch,
            maxRetries: $this->maxRetries,
            dedupTtlSeconds: $this->dedupTtlSeconds,
        );

        $safeHandler = function (array $payload, AMQPMessage $raw) use ($handler): void {
            try {
                $handler($payload, $raw);
                // Success — reset consecutive error counter.
                $this->consecutiveErrors = 0;
            } catch (ValidationException $e) {
                $this->handlePoisonMessage($payload, $raw, $e, 'validation_error');
            } catch (Throwable $e) {
                if ($this->isParsingError($e)) {
                    $this->handlePoisonMessage($payload, $raw, $e, 'parsing_error');
                    return;
                }

                // Infrastructure error — re-throw so AmqpConsumer applies nack/retry.
                $this->consecutiveErrors++;

                Log::channel('single')->warning('SafeAmqpConsumer: infrastructure error', [
                    'queue'              => $raw->getRoutingKey(),
                    'consecutive_errors' => $this->consecutiveErrors,
                    'max_allowed'        => $this->maxConsecutiveErrors,
                    'error'              => $e->getMessage(),
                    'class'              => $e::class,
                ]);

                if ($this->consecutiveErrors >= $this->maxConsecutiveErrors) {
                    // Signal the consume loop to stop after this message is nacked.
                    $this->shouldHalt = true;
                    $this->haltReason = new RuntimeException(
                        sprintf(
                            'SafeAmqpConsumer halted: %d consecutive infrastructure errors exceeded limit of %d. Last error: %s',
                            $this->consecutiveErrors,
                            $this->maxConsecutiveErrors,
                            $e->getMessage(),
                        ),
                        previous: $e,
                    );
                }

                throw $e;
            }
        };

        $combinedShouldStop = function () use ($shouldStop): bool {
            if ($this->shouldHalt) {
                return true;
            }
            return $shouldStop !== null && $shouldStop();
        };

        $inner->consume($queue, $safeHandler, $combinedShouldStop);

        // After the consume loop exits, propagate the halt reason so the caller
        // (e.g. an Artisan command) can log it and restart the process.
        if ($this->haltReason !== null) {
            throw $this->haltReason;
        }
    }

    /**
     * Clasifica el error como error de parsing/deserialización en el handler.
     * Los errores de JSON del body AMQP ya son capturados por AmqpConsumer
     * antes de invocar al handler, por lo que aquí solo llegamos con errores
     * que ocurren dentro de la lógica del handler propio.
     */
    private function isParsingError(Throwable $e): bool
    {
        return $e instanceof \JsonException
            || $e instanceof \UnexpectedValueException
            || str_contains($e->getMessage(), 'JSON')
            || str_contains($e->getMessage(), 'parse')
            || str_contains($e->getMessage(), 'decode');
    }

    /**
     * Gestiona un mensaje envenenado: log + callback personalizado.
     * Retorna sin relanzar para que AmqpConsumer ejecute ack() (drop definitivo).
     */
    private function handlePoisonMessage(array $payload, AMQPMessage $raw, Throwable $e, string $reason): void
    {
        $messageId = $raw->has('message_id') ? $raw->get('message_id') : null;

        Log::channel('single')->warning('SafeAmqpConsumer: poison message dropped', [
            'message_id' => $messageId,
            'reason'     => $reason,
            'error'      => $e->getMessage(),
            'class'      => $e::class,
        ]);

        if ($this->onPoisonMessage !== null) {
            ($this->onPoisonMessage)($payload, $e, $raw);
        }

        // No re-throw → AmqpConsumer executes ack() after handler returns.
        // This is the correct drop behaviour for a poison message.
    }
}

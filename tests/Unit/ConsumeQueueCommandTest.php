<?php

declare(strict_types=1);

use Illuminate\Console\OutputStyle;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Log;
use Maya\Messaging\Console\ConsumeQueueCommand;
use Maya\Messaging\Exceptions\UnrecoverableIngestionException;
use Maya\Messaging\Support\AmqpConsumer;
use Mockery as m;
use PhpAmqpLib\Message\AMQPMessage;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Create an OutputStyle compatible with Illuminate\Console\Command::setOutput().
 */
function makeOutputStyle(): OutputStyle
{
    return new OutputStyle(new ArrayInput([]), new NullOutput());
}

/**
 * Build a ConsumeQueueCommand subclass with injectable behavior.
 *
 * @param string           $queueName         Queue name returned by queueName().
 * @param \Throwable|null  $ingestException   If set, ingest() will throw it.
 * @param bool             &$flushCalled      Set to true when flush() is called.
 * @param array            &$ingestedPayloads Accumulates payloads passed to ingest().
 */
function makeCommand(
    string $queueName = 'test.queue',
    ?\Throwable $ingestException = null,
    bool &$flushCalled = false,
    array &$ingestedPayloads = [],
): ConsumeQueueCommand {
    return new class(
        $queueName,
        $ingestException,
        $flushCalled,
        $ingestedPayloads,
    ) extends ConsumeQueueCommand {
        protected $signature = 'test:consume {--queue=test.queue}';

        public function __construct(
            private readonly string $queueNameValue,
            private readonly ?\Throwable $ingestException,
            private bool &$flushCalledRef,
            private array &$ingestedPayloadsRef,
        ) {
            parent::__construct();
        }

        public function queueName(): string
        {
            return $this->queueNameValue;
        }

        public function ingest(array $payload, AMQPMessage $message): void
        {
            if ($this->ingestException !== null) {
                throw $this->ingestException;
            }
            $this->ingestedPayloadsRef[] = $payload;
        }

        public function flush(): void
        {
            $this->flushCalledRef = true;
        }
    };
}

/**
 * Run command->handle() with a fake AmqpConsumer that delivers one payload.
 */
function runCommandWithPayload(ConsumeQueueCommand $command, array $payload): int
{
    $consumer = m::mock(AmqpConsumer::class);
    $consumer
        ->shouldReceive('consume')
        ->once()
        ->andReturnUsing(function (string $queue, \Closure $handler) use ($payload): void {
            $raw = m::mock(AMQPMessage::class);
            $handler($payload, $raw);
        });

    $command->setLaravel(app());
    $command->setOutput(makeOutputStyle());

    return $command->handle($consumer);
}

/**
 * Run command->handle() with an idle consumer (no messages delivered).
 */
function runCommandIdle(ConsumeQueueCommand $command): int
{
    $consumer = m::mock(AmqpConsumer::class);
    $consumer->shouldReceive('consume')->once()->andReturn(null);

    $command->setLaravel(app());
    $command->setOutput(makeOutputStyle());

    return $command->handle($consumer);
}

afterEach(fn () => m::close());

// ---------------------------------------------------------------------------
// Tests: queueName() — hook wires queue to AmqpConsumer::consume()
// ---------------------------------------------------------------------------

it('passes queueName() return value as first argument to AmqpConsumer::consume()', function (): void {
    $consumer = m::mock(AmqpConsumer::class);
    $consumer
        ->shouldReceive('consume')
        ->once()
        ->with('custom.queue', m::type(\Closure::class))
        ->andReturn(null);

    $command = makeCommand(queueName: 'custom.queue');
    $command->setLaravel(app());
    $command->setOutput(makeOutputStyle());

    $command->handle($consumer);
});

// ---------------------------------------------------------------------------
// Tests: handle() returns SUCCESS
// ---------------------------------------------------------------------------

it('returns Command::SUCCESS after the consume loop exits', function (): void {
    $command = makeCommand();
    $exitCode = runCommandIdle($command);

    expect($exitCode)->toBe(\Illuminate\Console\Command::SUCCESS);
});

// ---------------------------------------------------------------------------
// Tests: ingest() hook — happy path, payload is forwarded
// ---------------------------------------------------------------------------

it('calls ingest() with the decoded payload on success', function (): void {
    $ingested = [];
    $command = makeCommand(ingestedPayloads: $ingested);

    runCommandWithPayload($command, ['event' => 'test', 'user_id' => 42]);

    expect($ingested)->toHaveCount(1)
        ->and($ingested[0]['event'])->toBe('test')
        ->and($ingested[0]['user_id'])->toBe(42);
});

// ---------------------------------------------------------------------------
// Tests: ACK policy — UnrecoverableIngestionException → drop (no rethrow)
// ---------------------------------------------------------------------------

it('does not rethrow UnrecoverableIngestionException (consumer ACKs = drop)', function (): void {
    $exception = new UnrecoverableIngestionException('unknown app slug');
    $command = makeCommand(ingestException: $exception);

    $exitCode = runCommandWithPayload($command, ['bad' => 'payload']);

    expect($exitCode)->toBe(\Illuminate\Console\Command::SUCCESS);
});

it('logs a warning when dropping an UnrecoverableIngestionException', function (): void {
    Log::shouldReceive('warning')
        ->once()
        ->withArgs(function (string $message): bool {
            return str_contains($message, 'dropping') || str_contains($message, 'unrecoverable');
        });
    Log::shouldReceive('error')->zeroOrMoreTimes();

    $exception = new UnrecoverableIngestionException('bad slug');
    $command = makeCommand(ingestException: $exception);

    runCommandWithPayload($command, ['bad' => 'data']);
});

// ---------------------------------------------------------------------------
// Tests: NACK policy — QueryException → rethrow so consumer NACKs
// ---------------------------------------------------------------------------

it('rethrows QueryException so AmqpConsumer NACKs and retries', function (): void {
    $qe = new QueryException('pgsql', 'SELECT 1', [], new \Exception('conn refused'));
    $command = makeCommand(ingestException: $qe);

    // Wire a consumer that verifies the rethrow happened.
    $consumer = m::mock(AmqpConsumer::class);
    $consumer
        ->shouldReceive('consume')
        ->once()
        ->andReturnUsing(function (string $queue, \Closure $handler): void {
            $raw = m::mock(AMQPMessage::class);
            try {
                $handler(['some' => 'data'], $raw);
            } catch (QueryException $e) {
                // Expected rethrow — NACK logic would kick in here in the real AmqpConsumer.
                throw $e;
            }
        });

    $command->setLaravel(app());
    $command->setOutput(makeOutputStyle());

    expect(fn () => $command->handle($consumer))->toThrow(QueryException::class);
});

// ---------------------------------------------------------------------------
// Tests: unexpected Throwable → drop, report()
// ---------------------------------------------------------------------------

it('drops unexpected Throwable and returns SUCCESS (no rethrow)', function (): void {
    $exception = new \RuntimeException('unexpected boom');
    $command = makeCommand(ingestException: $exception);

    $exitCode = runCommandWithPayload($command, ['x' => 1]);

    expect($exitCode)->toBe(\Illuminate\Console\Command::SUCCESS);
});

it('logs an error when dropping an unexpected Throwable', function (): void {
    $errorMessages = [];

    Log::shouldReceive('error')
        ->atLeast()->once()
        ->andReturnUsing(function (string $message) use (&$errorMessages): void {
            $errorMessages[] = $message;
        });
    Log::shouldReceive('warning')->zeroOrMoreTimes();

    $exception = new \RuntimeException('unexpected boom');
    $command = makeCommand(ingestException: $exception);

    runCommandWithPayload($command, ['x' => 1]);

    $matched = array_filter(
        $errorMessages,
        fn (string $m) => str_contains($m, 'unexpected') || str_contains($m, 'dropping'),
    );

    expect($matched)->not->toBeEmpty();
});

// ---------------------------------------------------------------------------
// Tests: flush() is called after consume loop
// ---------------------------------------------------------------------------

it('calls flush() after the consume loop exits', function (): void {
    $flushCalled = false;
    $command = makeCommand(flushCalled: $flushCalled);

    runCommandIdle($command);

    expect($flushCalled)->toBeTrue();
});

it('calls flush() even after a successful message delivery', function (): void {
    $flushCalled = false;
    $ingested = [];
    $command = makeCommand(flushCalled: $flushCalled, ingestedPayloads: $ingested);

    runCommandWithPayload($command, ['e' => 'evt']);

    expect($flushCalled)->toBeTrue();
});

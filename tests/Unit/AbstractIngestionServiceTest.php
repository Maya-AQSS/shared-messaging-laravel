<?php

declare(strict_types=1);

use Maya\Messaging\Contracts\IngestionService;
use Maya\Messaging\Exceptions\UnrecoverableIngestionException;
use Maya\Messaging\Services\AbstractIngestionService;

// ---------------------------------------------------------------------------
// Concrete subclass for testing — minimal DTO + persist spy.
// ---------------------------------------------------------------------------

/**
 * DTO returned by parse() in the test double.
 */
readonly class FakeDto
{
    public function __construct(public readonly string $name) {}
}

/**
 * Concrete service that uses a closure to simulate parse/persist behavior.
 */
class SpyIngestionService extends AbstractIngestionService
{
    public array $persisted = [];

    public bool $parseThrows = false;

    public bool $persistThrows = false;

    protected function parse(array $payload): object
    {
        if ($this->parseThrows) {
            throw new \InvalidArgumentException('missing required field: name');
        }

        if (empty($payload['name'])) {
            throw new \InvalidArgumentException('name is required');
        }

        return new FakeDto($payload['name']);
    }

    protected function persist(object $dto): void
    {
        if ($this->persistThrows) {
            throw new \RuntimeException('DB connection failed');
        }

        $this->persisted[] = $dto;
    }
}

// ---------------------------------------------------------------------------
// Tests: ingest() implements IngestionService contract
// ---------------------------------------------------------------------------

it('implements IngestionService contract', function (): void {
    $service = new SpyIngestionService();

    expect($service)->toBeInstanceOf(IngestionService::class);
});

// ---------------------------------------------------------------------------
// Tests: happy path — parse succeeds and persist is called
// ---------------------------------------------------------------------------

it('calls persist() with parsed DTO on valid payload', function (): void {
    $service = new SpyIngestionService();
    $service->ingest(['name' => 'test-event']);

    expect($service->persisted)->toHaveCount(1)
        ->and($service->persisted[0])->toBeInstanceOf(FakeDto::class)
        ->and($service->persisted[0]->name)->toBe('test-event');
});

// ---------------------------------------------------------------------------
// Tests: parse failure — InvalidArgumentException is logged and dropped
// ---------------------------------------------------------------------------

it('drops and logs when parse() throws InvalidArgumentException', function (): void {
    $service = new SpyIngestionService();
    $service->parseThrows = true;

    // Must not throw — the exception is absorbed.
    $service->ingest(['whatever' => 'junk']);

    expect($service->persisted)->toBeEmpty();
});

it('does not call persist() when parse() fails', function (): void {
    $service = new SpyIngestionService();

    // Missing required "name" → parse() throws InvalidArgumentException.
    $service->ingest([]);

    expect($service->persisted)->toBeEmpty();
});

// ---------------------------------------------------------------------------
// Tests: persist failure — rethrows so caller can NACK
// ---------------------------------------------------------------------------

it('rethrows exception from persist() so the caller can NACK', function (): void {
    $service = new SpyIngestionService();
    $service->persistThrows = true;

    expect(fn () => $service->ingest(['name' => 'valid']))
        ->toThrow(\RuntimeException::class, 'DB connection failed');
});

// ---------------------------------------------------------------------------
// Tests: UnrecoverableIngestionException from parse() is not caught by accident
// ---------------------------------------------------------------------------

it('propagates UnrecoverableIngestionException from parse() without wrapping', function (): void {
    $service = new class extends AbstractIngestionService {
        protected function parse(array $payload): object
        {
            throw new UnrecoverableIngestionException('malformed');
        }

        protected function persist(object $dto): void {}
    };

    // UnrecoverableIngestionException is NOT InvalidArgumentException, so it
    // must propagate so the command-level handler can ACK (drop) it explicitly.
    expect(fn () => $service->ingest(['x' => 1]))
        ->toThrow(UnrecoverableIngestionException::class);
});

// ---------------------------------------------------------------------------
// Tests: parse() returning different DTO types (polymorphism)
// ---------------------------------------------------------------------------

it('forwards whatever parse() returns to persist()', function (): void {
    $captured = null;

    $service = new class($captured) extends AbstractIngestionService {
        public function __construct(private mixed &$capturedDto) {}

        protected function parse(array $payload): object
        {
            return (object) ['value' => $payload['v']];
        }

        protected function persist(object $dto): void
        {
            $this->capturedDto = $dto;
        }
    };

    $service->ingest(['v' => 99]);

    expect($captured->value)->toBe(99);
});

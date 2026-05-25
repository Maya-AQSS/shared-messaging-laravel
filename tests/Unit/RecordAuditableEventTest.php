<?php

use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Support\Facades\DB;
use Maya\Messaging\Contracts\AuditableEvent;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Listeners\RecordAuditableEvent;
use Maya\Messaging\Publishers\AuditPublisher;
use Mockery as m;

afterEach(fn () => m::close());

/**
 * Event de prueba que implementa AuditableEvent.
 */
class FakeApprovedEvent implements AuditableEvent
{
    use Dispatchable;

    public function __construct(
        public readonly string $entityId,
        public readonly string $userId,
    ) {}

    public function toAuditPayload(): array
    {
        return [
            'applicationSlug' => 'test-app',
            'entityType'      => 'document',
            'entityId'        => $this->entityId,
            'action'          => 'approved',
            'userId'          => $this->userId,
            'newValue'        => ['status' => 'approved'],
        ];
    }
}

/**
 * Event que NO implementa AuditableEvent — el wildcard debe ignorarlo.
 */
class FakeUnrelatedEvent
{
    use Dispatchable;

    public function __construct(public readonly string $payload = 'noise') {}
}

// ─── Listener unit ─────────────────────────────────────────────────────────

it('RecordAuditableEvent::handle spreads toAuditPayload() into AuditPublisher::publish', function () {
    $capturedArgs = null;

    $messagePublisher = m::mock(MessagePublisher::class);
    $messagePublisher->shouldReceive('publish')
        ->andReturnUsing(function ($exchange, $routingKey, $payload) use (&$capturedArgs) {
            $capturedArgs = compact('exchange', 'routingKey', 'payload');
        });

    $auditPublisher = new AuditPublisher(publisher: $messagePublisher, exchange: 'maya.audit');
    $listener       = new RecordAuditableEvent($auditPublisher);

    $listener->handle(new FakeApprovedEvent(entityId: 'doc-42', userId: 'user-7'));

    expect($capturedArgs['routingKey'])->toBe('test-app.document.approved');
    expect($capturedArgs['payload'])->toMatchArray([
        'application_slug' => 'test-app',
        'entity_type'      => 'document',
        'entity_id'        => 'doc-42',
        'action'           => 'approved',
        'user_id'          => 'user-7',
        'new_value'        => ['status' => 'approved'],
    ]);
});

// ─── Wildcard integration ─────────────────────────────────────────────────

it('wildcard listener publishes AuditableEvent after dispatch (outside transaction)', function () {
    $capturedPayload = null;

    $messagePublisher = m::mock(MessagePublisher::class);
    $messagePublisher->shouldReceive('publish')
        ->andReturnUsing(function ($exchange, $key, $payload) use (&$capturedPayload) {
            $capturedPayload = $payload;
        });

    // Sustituir el binding del package para que el listener use nuestro mock.
    app()->instance(AuditPublisher::class, new AuditPublisher(
        publisher: $messagePublisher,
        exchange: 'maya.audit',
    ));

    FakeApprovedEvent::dispatch('doc-001', 'operator-1');

    expect($capturedPayload)->not->toBeNull();
    expect($capturedPayload['entity_id'])->toBe('doc-001');
    expect($capturedPayload['action'])->toBe('approved');
});

it('wildcard listener ignores Events that do not implement AuditableEvent', function () {
    $messagePublisher = m::mock(MessagePublisher::class);
    $messagePublisher->shouldNotReceive('publish');

    app()->instance(AuditPublisher::class, new AuditPublisher(
        publisher: $messagePublisher,
        exchange: 'maya.audit',
    ));

    FakeUnrelatedEvent::dispatch('noise');

    // Nada que verificar — Mockery fallará en `m::close()` si publish() se llamó.
    expect(true)->toBeTrue();
});

it('wildcard listener defers publishing until COMMIT', function () {
    $publishedCount = 0;

    $messagePublisher = m::mock(MessagePublisher::class);
    $messagePublisher->shouldReceive('publish')->andReturnUsing(function () use (&$publishedCount) {
        $publishedCount++;
    });

    app()->instance(AuditPublisher::class, new AuditPublisher(
        publisher: $messagePublisher,
        exchange: 'maya.audit',
    ));

    DB::transaction(function () use (&$publishedCount) {
        FakeApprovedEvent::dispatch('doc-tx', 'operator-tx');
        // Dentro de la transacción todavía no se ha publicado.
        expect($publishedCount)->toBe(0);
    });

    // Tras commit, exactamente una publicación.
    expect($publishedCount)->toBe(1);
});

it('wildcard listener does NOT publish if the transaction rolls back', function () {
    $publishedCount = 0;

    $messagePublisher = m::mock(MessagePublisher::class);
    $messagePublisher->shouldReceive('publish')->andReturnUsing(function () use (&$publishedCount) {
        $publishedCount++;
    });

    app()->instance(AuditPublisher::class, new AuditPublisher(
        publisher: $messagePublisher,
        exchange: 'maya.audit',
    ));

    try {
        DB::transaction(function () {
            FakeApprovedEvent::dispatch('doc-rollback', 'operator-rb');
            throw new RuntimeException('forced rollback');
        });
    } catch (RuntimeException) {
        // expected
    }

    expect($publishedCount)->toBe(0);
});

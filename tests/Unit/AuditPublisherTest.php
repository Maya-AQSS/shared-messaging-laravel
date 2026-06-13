<?php

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Jobs\RetryAmqpPublishJob;
use Maya\Messaging\Publishers\AuditPublisher;
use Mockery as m;

function makeAuditPublisher(?MessagePublisher $publisher = null): AuditPublisher
{
    $publisher ??= m::mock(MessagePublisher::class);
    return new AuditPublisher(
        publisher: $publisher,
        exchange: 'maya.audit',
    );
}

afterEach(fn () => m::close());

// ─── Payload shape ───────────────────────────────────────────────────────

it('includes occurred_at in the published payload', function () {
    $capturedPayload = null;

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($exchange, $key, $payload) use (&$capturedPayload) {
        $capturedPayload = $payload;
    });

    $ap = makeAuditPublisher($publisher);
    $ap->publish(
        applicationSlug: 'maya_test',
        entityType: 'document',
        entityId: 'doc-001',
        action: 'create',
        userId: 'user-uuid-001',
    );

    expect($capturedPayload)->toHaveKey('occurred_at');
    expect($capturedPayload['occurred_at'])->toMatch('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/');
});

it('includes all required fields in payload', function () {
    $capturedPayload = null;

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($ex, $key, $payload) use (&$capturedPayload) {
        $capturedPayload = $payload;
    });

    makeAuditPublisher($publisher)->publish(
        applicationSlug: 'maya_auth',
        entityType: 'user',
        entityId: 'u-001',
        action: 'login',
        userId: 'operator-uuid',
        ipAddress: '127.0.0.1',
    );

    expect($capturedPayload)->toMatchArray([
        'application_slug' => 'maya_auth',
        'entity_type'      => 'user',
        'entity_id'        => 'u-001',
        'action'           => 'login',
        'user_id'          => 'operator-uuid',
        'ip_address'       => '127.0.0.1',
    ]);
});

it('omits null optional fields from payload (array_filter)', function () {
    $capturedPayload = null;

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($ex, $key, $payload) use (&$capturedPayload) {
        $capturedPayload = $payload;
    });

    makeAuditPublisher($publisher)->publish('app', 'doc', 'id', 'view', 'user-id');

    expect($capturedPayload)->not->toHaveKey('block_id');
    expect($capturedPayload)->not->toHaveKey('ip_address');
    expect($capturedPayload)->not->toHaveKey('user_agent');
    expect($capturedPayload)->not->toHaveKey('previous_value');
    expect($capturedPayload)->not->toHaveKey('new_value');
    // occurred_at must always be present
    expect($capturedPayload)->toHaveKey('occurred_at');
});

it('uses correct routing key: slug.entity.action', function () {
    $capturedKey = null;

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($ex, $key) use (&$capturedKey) {
        $capturedKey = $key;
    });

    makeAuditPublisher($publisher)->publish('maya-dms', 'document', 'doc-1', 'delete', 'user-1');

    expect($capturedKey)->toBe('maya-dms.document.delete');
});

// ─── Fallback ────────────────────────────────────────────────────────────

it('dispatches RetryAmqpPublishJob when publish fails', function () {
    Queue::fake();

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andThrow(new RuntimeException('AMQP down'));

    Log::shouldReceive('warning')->once();

    makeAuditPublisher($publisher)->publish('app', 'entity', 'id', 'action', 'user');

    Queue::assertPushed(RetryAmqpPublishJob::class);
});

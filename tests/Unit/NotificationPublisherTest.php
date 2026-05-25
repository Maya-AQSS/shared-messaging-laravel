<?php

use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Queue;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Jobs\RetryAmqpPublishJob;
use Maya\Messaging\Publishers\NotificationPublisher;
use Mockery as m;

function makeNotificationPublisher(?MessagePublisher $publisher = null): NotificationPublisher
{
    $publisher ??= m::mock(MessagePublisher::class);
    return new NotificationPublisher(
        publisher: $publisher,
        exchange: 'maya.notifications',
        defaultApp: 'test-app',
    );
}

afterEach(fn () => m::close());

// ─── Payload shape ───────────────────────────────────────────────────────

it('publishes recipient_keycloak_id (UUID string) not recipient_id (int)', function () {
    $capturedPayload = null;

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($exchange, $key, $payload, $props) use (&$capturedPayload) {
        $capturedPayload = $payload;
    });

    $np = makeNotificationPublisher($publisher);
    $np->send('alert', 'keycloak-uuid-1234-5678', 'Title', 'Body');

    expect($capturedPayload)->toHaveKey('recipient_keycloak_id');
    expect($capturedPayload['recipient_keycloak_id'])->toBe('keycloak-uuid-1234-5678');
    expect($capturedPayload)->not->toHaveKey('recipient_id');
});

it('publishes to one routing key per channel', function () {
    $routingKeys = [];

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($exchange, $key) use (&$routingKeys) {
        $routingKeys[] = $key;
    });

    $np = makeNotificationPublisher($publisher);
    $np->send('info', 'uuid-0001', 'Title', 'Body', ['app', 'email']);

    expect($routingKeys)->toHaveCount(2);
    expect($routingKeys[0])->toBe('test-app.info.app');
    expect($routingKeys[1])->toBe('test-app.info.email');
});

it('uses same message_id across all channel publishes', function () {
    $messageIds = [];

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andReturnUsing(function ($exchange, $key, $payload, $props) use (&$messageIds) {
        $messageIds[] = $props['message_id'] ?? null;
    });

    $np = makeNotificationPublisher($publisher);
    $np->send('alert', 'uuid-0002', 'T', 'B', ['app', 'email', 'webhook']);

    expect(array_unique($messageIds))->toHaveCount(1);
    expect($messageIds[0])->toMatch('/^[0-9a-f-]{36}$/');
});

it('throws when channels list is empty', function () {
    $np = makeNotificationPublisher();
    expect(fn () => $np->send('info', 'uuid', 'T', 'B', []))->toThrow(InvalidArgumentException::class);
});

it('throws when channel is not in VALID_CHANNELS', function () {
    $np = makeNotificationPublisher();
    expect(fn () => $np->send('info', 'uuid', 'T', 'B', ['telegram']))->toThrow(InvalidArgumentException::class);
});

// ─── Fallback to RetryAmqpPublishJob on publish failure ──────────────────

it('dispatches RetryAmqpPublishJob when publish throws', function () {
    Queue::fake();

    $publisher = m::mock(MessagePublisher::class);
    $publisher->shouldReceive('publish')->andThrow(new RuntimeException('AMQP connection lost'));

    Log::shouldReceive('warning')->once();

    $np = makeNotificationPublisher($publisher);
    $np->send('alert', 'uuid', 'T', 'B', ['app']);

    Queue::assertPushed(RetryAmqpPublishJob::class);
});

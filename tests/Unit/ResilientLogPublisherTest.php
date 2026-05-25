<?php

declare(strict_types=1);

use Illuminate\Log\Events\MessageLogged;
use Illuminate\Support\Facades\Event;
use Maya\Messaging\Publishers\LogPublisher;
use Maya\Messaging\Publishers\ResilientLogPublisher;

beforeEach(function (): void {
    config(['logging.default' => 'null']);
    $this->app->forgetInstance('log');
});

it('registra warning y no relanza si LogPublisher falla', function (): void {
    Event::fake([MessageLogged::class]);

    $logPublisher = $this->createMock(LogPublisher::class);
    $logPublisher->expects($this->once())
        ->method('publish')
        ->willThrowException(new RuntimeException('broker caído'));

    $original = new RuntimeException('error de negocio');

    $sut = new ResilientLogPublisher($logPublisher);
    $sut->publishFromThrowable($original, 'medium', 'LAR-AUTH-001', ['k' => 1], 'maya-authorization');

    Event::assertDispatched(MessageLogged::class, function (MessageLogged $event): bool {
        return $event->level === 'warning'
            && $event->message === 'maya.logs.publish_failed_after_operation_failure'
            && ($event->context['app'] ?? null) === 'maya-authorization'
            && ($event->context['error_code'] ?? null) === 'LAR-AUTH-001';
    });
});

it('delega en LogPublisher sin warning resiliente cuando publish tiene éxito', function (): void {
    Event::fake([MessageLogged::class]);

    $logPublisher = $this->createMock(LogPublisher::class);
    $logPublisher->expects($this->once())->method('publish');

    $sut = new ResilientLogPublisher($logPublisher);
    $sut->publishFromThrowable(new RuntimeException('x'), 'low', 'code', [], 'maya-authorization');

    Event::assertNotDispatched(MessageLogged::class, function (MessageLogged $event): bool {
        return $event->message === 'maya.logs.publish_failed_after_operation_failure';
    });
});

it('publishStructured loggea warning resiliente cuando publish falla', function (): void {
    Event::fake([MessageLogged::class]);

    $logPublisher = $this->createMock(LogPublisher::class);
    $logPublisher->method('publish')->willThrowException(new RuntimeException('broker caído'));

    $sut = new ResilientLogPublisher($logPublisher);
    $sut->publishStructured('high', 'algo malo', 'LAR-XXX-002', ['ctx' => 'foo'], 'maya-logs');

    Event::assertDispatched(MessageLogged::class, function (MessageLogged $event): bool {
        return $event->level === 'warning'
            && $event->message === 'maya.logs.publish_failed_after_operation_failure'
            && ($event->context['original_message'] ?? null) === 'algo malo';
    });
});

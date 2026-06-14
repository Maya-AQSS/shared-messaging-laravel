<?php

use Illuminate\Contracts\Foundation\Application;
use Maya\Messaging\Providers\MessagingServiceProvider;

function invokeAssert(bool $isTesting): void
{
    $app = Mockery::mock(Application::class);
    $app->shouldReceive('environment')->with('testing')->andReturn($isTesting);

    $provider = new MessagingServiceProvider($app);
    $method = new ReflectionMethod($provider, 'assertAmqpCredentialsConfigured');
    $method->setAccessible(true);
    $method->invoke($provider);
}

afterEach(function () {
    Mockery::close();
});

it('throws when AMQP credentials are missing outside testing', function () {
    config(['messaging.connection.user' => null, 'messaging.connection.password' => null]);

    expect(fn () => invokeAssert(false))->toThrow(RuntimeException::class);
});

it('throws when only the password is missing outside testing', function () {
    config(['messaging.connection.user' => 'maya_logs', 'messaging.connection.password' => '']);

    expect(fn () => invokeAssert(false))->toThrow(RuntimeException::class);
});

it('passes when both credentials are present outside testing', function () {
    config(['messaging.connection.user' => 'maya_logs', 'messaging.connection.password' => 'a-secret']);

    invokeAssert(false);

    expect(true)->toBeTrue(); // no exception thrown
});

it('skips the check entirely in the testing environment', function () {
    config(['messaging.connection.user' => null, 'messaging.connection.password' => null]);

    invokeAssert(true);

    expect(true)->toBeTrue(); // no exception even with missing creds
});

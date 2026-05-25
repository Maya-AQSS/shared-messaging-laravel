<?php

namespace Tests;

use Maya\Messaging\Providers\MessagingServiceProvider;
use Orchestra\Testbench\TestCase as OrchestraTestCase;

abstract class TestCase extends OrchestraTestCase
{
    protected function getPackageProviders($app): array
    {
        return [
            MessagingServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('cache.default', 'array');
        $app['config']->set('messaging.app', 'test-app');
        $app['config']->set('messaging.exchanges.audit', 'maya.audit');
        $app['config']->set('messaging.exchanges.logs', 'maya.logs');
        $app['config']->set('messaging.exchanges.notifications', 'maya.notifications');
        $app['config']->set('messaging.exchanges.alerts', 'maya.alerts');
        $app['config']->set('messaging.connection', [
            'host'     => 'localhost',
            'port'     => 5672,
            'user'     => 'guest',
            'password' => 'guest',
            'vhost'    => '/',
        ]);
    }
}

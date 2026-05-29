<?php

namespace Maya\Messaging\Providers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use Maya\Messaging\Contracts\AuditableEvent;
use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Listeners\RecordAuditableEvent;
use Maya\Messaging\Publishers\AlertPublisher;
use Maya\Messaging\Publishers\AuditPublisher;
use Maya\Messaging\Publishers\LogPublisher;
use Maya\Messaging\Publishers\NotificationPublisher;
use Maya\Messaging\Publishers\RabbitMQPublisher;
use Maya\Messaging\Support\AmqpConnectionFactory;

class MessagingServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(
            path: __DIR__ . '/../../config/messaging.php',
            key: 'messaging',
        );

        $this->app->singleton(AmqpConnectionFactory::class, function ($app) {
            return new AmqpConnectionFactory(config('messaging.connection'));
        });

        $this->app->singleton(MessagePublisher::class, function ($app) {
            return new RabbitMQPublisher(
                factory: $app->make(AmqpConnectionFactory::class),
                appId: config('messaging.app'),
                publisherConfirm: (bool) config('messaging.publish.confirm', false),
                mandatory: (bool) config('messaging.publish.mandatory', false),
            );
        });

        $this->app->singleton(LogPublisher::class, function ($app) {
            return new LogPublisher(
                publisher: $app->make(MessagePublisher::class),
                exchange: config('messaging.exchanges.logs'),
                defaultApp: config('messaging.app'),
            );
        });

        $this->app->singleton(NotificationPublisher::class, function ($app) {
            return new NotificationPublisher(
                publisher: $app->make(MessagePublisher::class),
                exchange: config('messaging.exchanges.notifications'),
                defaultApp: config('messaging.app'),
            );
        });

        $this->app->singleton(AlertPublisher::class, function ($app) {
            return new AlertPublisher(
                publisher: $app->make(MessagePublisher::class),
                exchange: config('messaging.exchanges.alerts'),
                notificationPublisher: $app->make(NotificationPublisher::class),
            );
        });

        $this->app->singleton(AuditPublisher::class, function ($app) {
            return new AuditPublisher(
                publisher: $app->make(MessagePublisher::class),
                exchange: config('messaging.exchanges.audit'),
            );
        });
    }

    public function boot(): void
    {
        $this->loadMigrationsFrom(__DIR__ . '/../../database/migrations');

        $this->publishes([
            __DIR__ . '/../../config/messaging.php' => config_path('messaging.php'),
        ], 'maya-messaging-config');

        // Wildcard de auditoría: cualquier Event que implemente AuditableEvent
        // se publica al exchange maya.audit tras commit (o inmediatamente si no
        // hay transacción activa). Patrón canónico definido en events.md.
        Event::listen('*', static function (string $eventName, array $payload): void {
            $event = $payload[0] ?? null;
            if (! $event instanceof AuditableEvent) {
                return;
            }

            DB::afterCommit(static fn () => app(RecordAuditableEvent::class)->handle($event));
        });
    }
}

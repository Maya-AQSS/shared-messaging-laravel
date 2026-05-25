# maya/shared-messaging-laravel

Paquete compartido para integrar las apps Laravel del ecosistema Maya con RabbitMQ (exchanges `maya.logs`, `maya.notifications`, `maya.alerts`).

## Instalación

En el `composer.json` de la app consumidora:

```json
{
  "require": {
    "maya/shared-messaging-laravel": "@dev"
  },
  "repositories": [
    { "type": "path", "url": "../packages/maya-shared-messaging-laravel" }
  ]
}
```

Publicar config:

```bash
php artisan vendor:publish --tag=maya-messaging-config
```

Variables de entorno (`.env`):

```
RABBITMQ_HOST=maya_rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=admin
RABBITMQ_VHOST=/
MAYA_MESSAGING_APP=maya-dms
```

## Uso

### Publicar un log

```php
use Maya\Messaging\Publishers\LogPublisher;

app(LogPublisher::class)->publish(
    severity: 'critical',
    message: 'DB timeout creating invoice',
    errorCode: 'E_INVOICE_TIMEOUT',
    metadata: ['user_id' => auth()->id()],
);
```

### Publicar una notificación

```php
use Maya\Messaging\Publishers\NotificationPublisher;

app(NotificationPublisher::class)->send(
    type: 'user.invited',
    recipientId: 42,
    title: 'Invitación al centro',
    body: 'Has sido invitado a colaborar.',
    channels: ['app', 'email'],
);
```

### Publicar una alerta

```php
use Maya\Messaging\Publishers\AlertPublisher;

app(AlertPublisher::class)->publish(
    ruleId: 'error-spike-dms',
    severity: 'critical',
    title: 'Pico de errores críticos en maya_dms',
    source: 'logs.aggregation',
    context: ['count' => 42, 'window_seconds' => 60],
);
```

### Monolog handler (logs → rabbit)

Añadir al `config/logging.php` de la app:

```php
'channels' => [
    'stack' => [
        'driver' => 'stack',
        'channels' => ['daily', 'rabbit'],
    ],
    'rabbit' => [
        'driver' => 'custom',
        'via' => \Maya\Messaging\Logging\RabbitMQLogChannel::class,
        'level' => env('LOG_RABBIT_LEVEL', 'warning'),
    ],
],
```

Cualquier `Log::error('...')` se publicará automáticamente en `maya.logs`.

### Escribir un consumer

```php
use Maya\Messaging\Support\AmqpConsumer;

class ConsumeNotifications extends Command
{
    protected $signature = 'notifications:consume';

    public function handle(AmqpConsumer $consumer): int
    {
        $consumer->consume('notifications.ingest', function (array $payload) {
            Notification::insertIgnore([
                'app'          => $payload['app'],
                'type'         => $payload['type'],
                'recipient_id' => $payload['recipient_id'],
                // ...
            ]);
        });
        return 0;
    }
}
```

El consumer gestiona ACK, retries (3) con caída automática a DLQ, dedupe por `message_id`, y señales SIGTERM/SIGINT para shutdown limpio.

## Contratos

Los contratos JSON de cada exchange están documentados en `DOCUMENTATION/docs/src/infraestructura/servicios/rabbitmq-contracts.md`.

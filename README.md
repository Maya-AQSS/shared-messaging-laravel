# ceedcv-maya/shared-messaging-laravel

RabbitMQ messaging layer for Laravel: typed event publishers (audit, logs, notifications, alerts), reusable consumer base, retry/DLX handling.

Part of the [ceedcv-maya/maya_platform](https://github.com/Maya-AQSS/maya_platform) mono-repo. Distributed independently for reuse outside the Maya ecosystem.

## Installation

```bash
composer require ceedcv-maya/shared-messaging-laravel
```

```php
use Maya\Messaging\Publishers\AuditPublisher;

AuditPublisher::dispatch([
    'app' => 'orders',
    'action' => 'create',
    'entity_type' => 'order',
    'entity_id' => $order->id,
    'user_id' => auth()->id(),
]);
```

```env
RABBITMQ_HOST=rabbitmq.example.org
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
```


## TypeScript / build notes
PSR-4 autoload from `src/`. Service providers are registered via Laravel package discovery (no manual provider registration needed).

## License

MIT — see [LICENSE](LICENSE).

## Reporting issues

The canonical source lives in [Maya-AQSS/maya_platform](https://github.com/Maya-AQSS/maya_platform). File issues there; this read-only split repo is only the published artifact.

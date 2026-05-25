<?php

namespace Maya\Messaging\Logging;

use Illuminate\Log\Logger;
use Maya\Messaging\Publishers\LogPublisher;
use Monolog\Level;
use Monolog\Logger as MonologLogger;

/**
 * Factory invoked by Laravel's logging.php when using:
 *   'rabbit' => ['driver' => 'custom', 'via' => RabbitMQLogChannel::class, ...]
 *
 * Keeps the handler wiring out of the consuming app's config — it only
 * specifies the level.
 */
class RabbitMQLogChannel
{
    public function __invoke(array $config): Logger
    {
        $publisher = app(LogPublisher::class);

        $levelMap = config('messaging.logging.map_levels', []);
        $minimumLevel = $config['level'] ?? config('messaging.logging.minimum_level', 'warning');

        $handler = new RabbitMQLogHandler(
            publisher: $publisher,
            levelMap: $levelMap,
            level: Level::fromName(ucfirst($minimumLevel)),
            bubble: (bool) ($config['bubble'] ?? true),
        );

        $monolog = new MonologLogger($config['name'] ?? 'rabbit');
        $monolog->pushHandler($handler);

        return new Logger($monolog);
    }
}

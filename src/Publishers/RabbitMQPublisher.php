<?php

namespace Maya\Messaging\Publishers;

use Maya\Messaging\Contracts\MessagePublisher;
use Maya\Messaging\Support\AmqpConnectionFactory;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Ramsey\Uuid\Uuid;

/**
 * Default MessagePublisher backed by php-amqplib.
 *
 * Every published message carries:
 *   - content_type   = application/json
 *   - delivery_mode  = 2 (persistent)
 *   - message_id     = UUID v4 (idempotency key for consumers)
 *   - timestamp      = unix epoch seconds
 *   - app_id         = config('messaging.app')
 *
 * Channel lifecycle is managed by AmqpConnectionFactory::getPublisherChannel(),
 * which keeps a single channel open for the lifetime of the factory singleton.
 */
class RabbitMQPublisher implements MessagePublisher
{
    public function __construct(
        private readonly AmqpConnectionFactory $factory,
        private readonly string $appId,
        private readonly bool $publisherConfirm = false,
        private readonly bool $mandatory = false,
    ) {}

    public function publish(string $exchange, string $routingKey, array $payload, array $properties = []): void
    {
        if ($exchange === '' || $routingKey === '') {
            throw new \InvalidArgumentException(
                "RabbitMQPublisher: exchange and routingKey must not be empty (exchange='{$exchange}', routingKey='{$routingKey}')"
            );
        }

        $channel = $this->factory->getPublisherChannel($this->publisherConfirm);

        $body = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

        $messageProperties = array_merge([
            'content_type'  => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'message_id'    => Uuid::uuid4()->toString(),
            'timestamp'     => time(),
            'app_id'        => $this->appId,
            'application_headers' => new AMQPTable([
                'x-maya-schema-version' => '1',
            ]),
        ], $properties);

        $message = new AMQPMessage($body, $messageProperties);

        $channel->basic_publish(
            msg: $message,
            exchange: $exchange,
            routing_key: $routingKey,
            mandatory: $this->mandatory,
        );

        // Only wait synchronously in CLI (queue workers/consumers).
        // In HTTP requests the blocking ack wait would add latency to every response.
        if ($this->publisherConfirm && app()->runningInConsole()) {
            $channel->wait_for_pending_acks(3.0);
        }
    }
}

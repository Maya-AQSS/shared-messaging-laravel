<?php

namespace Maya\Messaging\Contracts;

interface MessagePublisher
{
    /**
     * Publish an arbitrary payload to the given exchange + routing key.
     *
     * @param  string                $exchange
     * @param  string                $routingKey
     * @param  array<string, mixed>  $payload   JSON-serializable
     * @param  array<string, mixed>  $properties Optional AMQP overrides
     */
    public function publish(string $exchange, string $routingKey, array $payload, array $properties = []): void;
}

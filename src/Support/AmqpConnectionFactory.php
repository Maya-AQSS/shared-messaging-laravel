<?php

namespace Maya\Messaging\Support;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AbstractConnection;

/**
 * Lazy-connected, reusable connection to RabbitMQ.
 *
 * Uses AMQPLazyConnection so the actual TCP connection is deferred until the
 * first publish — important for CLI commands and short-lived HTTP handlers
 * that may never publish.
 *
 * Provides a shared publishing channel via getPublisherChannel(), separate
 * from consumer channels which are managed by AmqpConsumer directly.
 */
class AmqpConnectionFactory
{
    private ?AbstractConnection $connection = null;
    private ?AMQPChannel $publisherChannel = null;
    private bool $publisherConfirmSelected = false;

    public function __construct(
        private readonly array $config,
    ) {
        $heartbeat = $this->config['heartbeat'] ?? 60;
        if ($heartbeat < 30) {
            throw new \InvalidArgumentException(
                "AMQP heartbeat must be >= 30 seconds to avoid silent connection drops; got {$heartbeat}s. Set RABBITMQ_HEARTBEAT >= 30."
            );
        }
    }

    public function connection(): AbstractConnection
    {
        if ($this->connection === null || !$this->connection->isConnected()) {
            $this->connection = new AMQPLazyConnection(
                host:              $this->config['host'],
                port:              $this->config['port'],
                user:              $this->config['user'],
                password:          $this->config['password'],
                vhost:             $this->config['vhost'] ?? '/',
                heartbeat:         $this->config['heartbeat'] ?? 60,
                connection_timeout: $this->config['connection_timeout'] ?? 3.0,
                read_write_timeout: $this->config['read_write_timeout'] ?? 3.0,
            );
        }

        return $this->connection;
    }

    /**
     * Returns the shared publishing channel, opening it lazily and calling
     * confirm_select once if $confirm is true. Reopens automatically if closed.
     */
    public function getPublisherChannel(bool $confirm = false): AMQPChannel
    {
        if ($this->publisherChannel === null || !$this->publisherChannel->is_open()) {
            $this->publisherChannel = $this->connection()->channel();
            $this->publisherConfirmSelected = false;
        }

        if ($confirm && !$this->publisherConfirmSelected) {
            $this->publisherChannel->confirm_select();
            $this->publisherConfirmSelected = true;
        }

        return $this->publisherChannel;
    }

    public function close(): void
    {
        if ($this->publisherChannel !== null && $this->publisherChannel->is_open()) {
            $this->publisherChannel->close();
        }
        $this->publisherChannel = null;
        $this->publisherConfirmSelected = false;

        if ($this->connection !== null && $this->connection->isConnected()) {
            $this->connection->close();
        }
        $this->connection = null;
    }
}

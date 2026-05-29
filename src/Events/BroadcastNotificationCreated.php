<?php

declare(strict_types=1);

namespace Maya\Messaging\Events;

use Illuminate\Broadcasting\InteractsWithSockets;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcastNow;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

/**
 * Canonical broadcast event for realtime notifications across the Maya ecosystem.
 *
 * Dispatch this alongside a NotificationPublisher::send() call when a service
 * wants the recipient's open session to receive a push immediately, without
 * waiting for the dashboard ingestion + 60s frontend poll cycle.
 *
 *   Channel:    private notifications.{recipientId}     (Keycloak UUID)
 *   Event name: notification.created                    (matches dashboard
 *               local event so a single hook covers both sources)
 *
 * The payload mirrors the shape persisted in the dashboard `notifications`
 * table — app/type/title/body/metadata/is_critical/scope — so the frontend
 * treats WebSocket pushes and polled rows with one schema.
 */
final class BroadcastNotificationCreated implements ShouldBroadcastNow
{
    use Dispatchable;
    use InteractsWithSockets;
    use SerializesModels;

    /**
     * @param  array<string,mixed>  $metadata
     */
    public function __construct(
        public readonly string $recipientId,
        public readonly string $app,
        public readonly string $type,
        public readonly string $title,
        public readonly string $body,
        public readonly array $metadata = [],
        public readonly bool $isCritical = false,
        public readonly string $scope = 'user',
    ) {
    }

    public function broadcastOn(): PrivateChannel
    {
        return new PrivateChannel('notifications.' . $this->recipientId);
    }

    public function broadcastAs(): string
    {
        return 'notification.created';
    }

    /**
     * @return array<string,mixed>
     */
    public function broadcastWith(): array
    {
        return [
            'app' => $this->app,
            'type' => $this->type,
            'title' => $this->title,
            'body' => $this->body,
            'metadata' => $this->metadata,
            'is_critical' => $this->isCritical,
            'scope' => $this->scope,
        ];
    }
}

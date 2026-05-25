<?php

namespace Maya\Messaging\Listeners;

use Maya\Messaging\Contracts\AuditableEvent;
use Maya\Messaging\Publishers\AuditPublisher;

/**
 * Wildcard-invoked listener. Recibe cualquier Event que implemente
 * {@see AuditableEvent} y delega al AuditPublisher la publicación al
 * exchange `maya.audit`. El guard `DB::afterCommit()` vive en el
 * dispatch wildcard del {@see \Maya\Messaging\Providers\MessagingServiceProvider}.
 */
class RecordAuditableEvent
{
    public function __construct(
        private readonly AuditPublisher $publisher,
    ) {}

    public function handle(AuditableEvent $event): void
    {
        $this->publisher->publish(...$event->toAuditPayload());
    }
}

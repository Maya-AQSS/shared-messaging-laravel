<?php

namespace Maya\Messaging\Contracts;

interface AuditableEvent
{
    /**
     * Argumentos con nombre que AuditPublisher::publish() espera, listos para
     * spread (`...$event->toAuditPayload()`).
     *
     * Claves obligatorias: applicationSlug, entityType, entityId, action, userId.
     * Claves opcionales: blockId, previousValue, newValue, ipAddress, userAgent.
     *
     * @return array{
     *   applicationSlug: string,
     *   entityType: string,
     *   entityId: string,
     *   action: string,
     *   userId: string,
     *   blockId?: string|null,
     *   previousValue?: array<string, mixed>|null,
     *   newValue?: array<string, mixed>|null,
     *   ipAddress?: string|null,
     *   userAgent?: string|null,
     * }
     */
    public function toAuditPayload(): array;
}

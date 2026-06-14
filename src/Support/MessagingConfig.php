<?php

declare(strict_types=1);

namespace Maya\Messaging\Support;

/**
 * Tiny accessor for messaging configuration values, so services don't repeat
 * `(string) config('messaging.app')` in a private helper each. Replaces the
 * `messagingAppSlug()` private methods duplicated across app services.
 */
final class MessagingConfig
{
    /**
     * Slug of the current application as seen on the messaging bus
     * (config `messaging.app`, defaults to APP_NAME).
     */
    public static function appSlug(): string
    {
        return (string) config('messaging.app');
    }
}

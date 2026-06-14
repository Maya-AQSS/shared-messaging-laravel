<?php

use Maya\Messaging\Support\MessagingConfig;

it('returns the messaging.app config value as a string', function () {
    config(['messaging.app' => 'maya-dms']);

    expect(MessagingConfig::appSlug())->toBe('maya-dms');
});

it('casts non-string config values to string', function () {
    config(['messaging.app' => 123]);

    expect(MessagingConfig::appSlug())->toBe('123');
});

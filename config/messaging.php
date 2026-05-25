<?php

return [

    'app' => env('MAYA_MESSAGING_APP', env('APP_NAME', 'unknown')),

    'connection' => [
        'host'     => env('RABBITMQ_HOST', 'maya_rabbitmq'),
        'port'     => (int) env('RABBITMQ_PORT', 5672),
        'user'     => env('RABBITMQ_USER', 'admin'),
        'password' => env('RABBITMQ_PASSWORD', 'admin'),
        'vhost'    => env('RABBITMQ_VHOST', '/'),
        'heartbeat'        => (int) env('RABBITMQ_HEARTBEAT', 60),
        'connection_timeout' => (float) env('RABBITMQ_CONNECTION_TIMEOUT', 3.0),
        'read_write_timeout' => (float) env('RABBITMQ_RW_TIMEOUT', 3.0),
    ],

    'exchanges' => [
        'logs'          => env('MAYA_MESSAGING_EX_LOGS', 'maya.logs'),
        'notifications' => env('MAYA_MESSAGING_EX_NOTIFICATIONS', 'maya.notifications'),
        'alerts'        => env('MAYA_MESSAGING_EX_ALERTS', 'maya.alerts'),
        'audit'         => env('MAYA_MESSAGING_EX_AUDIT', 'maya.audit'),
    ],

    'publish' => [
        'confirm' => (bool) env('MAYA_MESSAGING_CONFIRM', true),
        'mandatory' => (bool) env('MAYA_MESSAGING_MANDATORY', false),
    ],

    'notifications' => [
        'valid_channels' => explode(',', env('MAYA_NOTIFICATION_CHANNELS', 'app,email,webhook,slack')),
    ],

    'logging' => [
        'channel_name' => env('MAYA_MESSAGING_LOG_CHANNEL', 'rabbit'),
        'minimum_level' => env('MAYA_MESSAGING_LOG_LEVEL', 'warning'),
        'map_levels' => [
            'emergency' => 'critical',
            'alert'     => 'critical',
            'critical'  => 'critical',
            'error'     => 'high',
            'warning'   => 'medium',
            'notice'    => 'low',
            'info'      => 'low',
            'debug'     => 'other',
        ],
    ],
];

<?php

declare(strict_types=1);

namespace Maya\Messaging\Observers\Concerns;

use Illuminate\Support\Carbon;
use Maya\Messaging\Publishers\AuditPublisher;

/**
 * Formatea instantes en payloads de auditoría con la misma regla que
 * {@see AuditPublisher} para occurred_at ({@see config('messaging.audit_timestamp_timezone')}).
 */
trait NormalizesAuditTemporalPayload
{
    /** @var list<string> */
    protected const AUDIT_ELOQUENT_TEMPORAL_KEYS = [
        'created_at',
        'updated_at',
        'deleted_at',
    ];

    /**
     * @param  array<string, mixed>|null  $payload
     * @param  list<string>  $temporalKeys
     * @return array<string, mixed>|null
     */
    protected function normalizeAuditTemporalPayload(?array $payload, array $temporalKeys): ?array
    {
        if ($payload === null) {
            return null;
        }

        foreach ($temporalKeys as $key) {
            if (! array_key_exists($key, $payload)) {
                continue;
            }
            $payload[$key] = $this->toAuditTemporalString($payload[$key]);
        }

        return $payload;
    }

    protected function toAuditTemporalString(mixed $value): mixed
    {
        if ($value === null || $value === '') {
            return $value;
        }

        try {
            $instant = $value instanceof \DateTimeInterface
                ? Carbon::instance($value)
                : Carbon::parse((string) $value);
            $instant = $instant->utc();

            $tz = config('messaging.audit_timestamp_timezone');
            if (is_string($tz) && $tz !== '') {
                try {
                    return $instant->timezone($tz)->toIso8601String();
                } catch (\Throwable) {
                    return $instant->toIso8601ZuluString();
                }
            }

            return $instant->toIso8601ZuluString();
        } catch (\Throwable) {
            return $value;
        }
    }
}

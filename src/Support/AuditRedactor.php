<?php

declare(strict_types=1);

namespace Maya\Messaging\Support;

/**
 * Redacts sensitive fields from audit payloads before publishing.
 *
 * Usage:
 *   $safe = AuditRedactor::redact($payload, ['password', 'token', 'secret']);
 */
final class AuditRedactor
{
    private const REDACTED = '[REDACTED]';

    /**
     * Returns a copy of $payload with the listed keys replaced by '[REDACTED]'.
     * Operates recursively on nested arrays.
     *
     * @param  array<string, mixed>|null  $payload
     * @param  list<string>               $sensitiveKeys  case-insensitive field names
     * @return array<string, mixed>|null
     */
    public static function redact(?array $payload, array $sensitiveKeys): ?array
    {
        if ($payload === null || $sensitiveKeys === []) {
            return $payload;
        }

        $lowerKeys = array_map('strtolower', $sensitiveKeys);
        return self::redactRecursive($payload, $lowerKeys);
    }

    /**
     * @param  array<string, mixed>  $data
     * @param  list<string>          $lowerKeys
     * @return array<string, mixed>
     */
    private static function redactRecursive(array $data, array $lowerKeys): array
    {
        $result = [];
        foreach ($data as $key => $value) {
            if (in_array(strtolower((string) $key), $lowerKeys, true)) {
                $result[$key] = self::REDACTED;
            } elseif (is_array($value)) {
                $result[$key] = self::redactRecursive($value, $lowerKeys);
            } else {
                $result[$key] = $value;
            }
        }
        return $result;
    }
}

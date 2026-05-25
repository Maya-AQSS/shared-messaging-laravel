<?php

declare(strict_types=1);

namespace Maya\Messaging\Observers;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Maya\Messaging\Observers\Concerns\NormalizesAuditTemporalPayload;
use Maya\Messaging\Publishers\AuditPublisher;

/**
 * Plantilla CRUD → maya.audit para modelos del panel admin.
 *
 * Observers CRUD por entidad publican created/updated/deleted al exchange
 * `maya.audit` vía {@see AuditPublisher}. La publicación se difiere a
 * `DB::afterCommit` para evitar fugar eventos cuando la transacción
 * upstream hace rollback.
 *
 * Cada observer concreto declara:
 *  - `auditEntityType()`: nombre lógico de la entidad (`document`, `comment`, …).
 *  - `auditTemporalKeys()`: campos del payload que deben formatearse como
 *    timestamp ISO-8601 (created_at, updated_at, deleted_at, expires_at, …).
 *  - `resolveAuditUserId()`: actor — habitualmente
 *    {@see self::resolvePanelActorUserId()}.
 *  - `auditSnapshot()` (opcional): payload legible en create/delete; por defecto
 *    atributos Eloquent crudos. Sobrescribir para enriquecer (nombres, slugs, …).
 *
 * Para deletes masivos, los repositorios deben borrar modelos uno a uno
 * para disparar `deleted()` (evitar `query->delete()` directo).
 */
abstract class AbstractAuditableModelObserver
{
    use NormalizesAuditTemporalPayload;

    public function __construct(
        protected readonly AuditPublisher $publisher,
    ) {}

    abstract protected function auditEntityType(): string;

    /**
     * Identificador estable del modelo para el evento de audit. Por defecto
     * usa la primary key. Modelos con PK compuesta (M2N de pivote) pueden
     * sobreescribir para componer un id sintético (`a:b`).
     */
    protected function auditEntityId(Model $model): string
    {
        $keyName = $model->getKeyName();

        if (is_array($keyName)) {
            $parts = array_map(
                static fn (string $col): string => (string) ($model->getAttribute($col) ?? ''),
                $keyName,
            );

            return implode(':', $parts);
        }

        return (string) $model->getKey();
    }

    /**
     * @return list<string>
     */
    abstract protected function auditTemporalKeys(): array;

    abstract protected function resolveAuditUserId(Model $model): string;

    /**
     * Payload de create/delete. Por defecto columnas del modelo; los observers
     * concretos pueden devolver un snapshot enriquecido.
     *
     * @return array<string, mixed>|null
     */
    protected function auditSnapshot(Model $model): ?array
    {
        return $model->getAttributes();
    }

    protected function auditAfterCreate(string $action, Model $model): void
    {
        $actorUserId = $this->resolveAuditUserId($model);

        $this->afterCommit(fn () => $this->publishAudit(
            $action,
            $model,
            null,
            $this->auditSnapshot($model),
            $actorUserId,
        ));
    }

    protected function auditAfterUpdate(string $action, Model $model): void
    {
        [$previous, $new] = $this->auditUpdateDiff($model);
        $actorUserId = $this->resolveAuditUserId($model);

        $this->afterCommit(fn () => $this->publishAudit(
            $action,
            $model,
            $previous,
            $new,
            $actorUserId,
        ));
    }

    protected function auditAfterDelete(string $action, Model $model): void
    {
        $actorUserId = $this->resolveAuditUserId($model);

        $this->afterCommit(fn () => $this->publishAudit(
            $action,
            $model,
            $this->auditSnapshot($model),
            null,
            $actorUserId,
        ));
    }

    /**
     * @return array{0: ?array<string, mixed>, 1: ?array<string, mixed>}
     */
    protected function auditUpdateDiff(Model $model): array
    {
        $previous = array_intersect_key($model->getOriginal(), $model->getChanges());
        $new = $model->getChanges();

        return [
            $previous !== [] ? $previous : null,
            $new !== [] ? $new : null,
        ];
    }

    protected function afterCommit(callable $callback): void
    {
        if (DB::transactionLevel() === 0) {
            $callback();

            return;
        }

        DB::afterCommit($callback);
    }

    protected function messagingAppSlug(): string
    {
        return (string) config('messaging.app');
    }

    /**
     * Actor del panel: claim `sub` del JWT (jwt_user) o usuario del guard api.
     */
    protected function resolvePanelActorUserId(?string $fallback = 'system'): string
    {
        $jwtUser = request()->attributes->get('jwt_user');
        if (is_array($jwtUser)) {
            $id = $jwtUser['id'] ?? null;
            if (is_string($id) && $id !== '') {
                return $id;
            }
        }

        $apiUser = auth('api')->user();
        if ($apiUser !== null) {
            $id = $apiUser->getAuthIdentifier();
            if (is_string($id) && $id !== '') {
                return $id;
            }
        }

        $user = request()->user();
        if ($user !== null) {
            $id = $user->getAuthIdentifier();
            if (is_string($id) && $id !== '') {
                return $id;
            }
        }

        return $fallback ?? 'system';
    }

    /**
     * @param  array<string, mixed>|null  $previousValue
     * @param  array<string, mixed>|null  $newValue
     */
    protected function publishAudit(
        string $action,
        Model $model,
        ?array $previousValue,
        ?array $newValue,
        ?string $actorUserId = null,
    ): void {
        if (! config('messaging.audit_enabled', true)) {
            return;
        }

        $this->publisher->publish(
            applicationSlug: $this->messagingAppSlug(),
            entityType: $this->auditEntityType(),
            entityId: $this->auditEntityId($model),
            action: $action,
            userId: $actorUserId ?? $this->resolveAuditUserId($model),
            previousValue: $this->normalizeAuditTemporalPayload($previousValue, $this->auditTemporalKeys()),
            newValue: $this->normalizeAuditTemporalPayload($newValue, $this->auditTemporalKeys()),
        );
    }
}

<?php

namespace Xtwoend\Model;

use Hyperf\Utils\Arr;
use Hyperf\Utils\Str;

trait Relation
{
    public function hasOne($related, $foreignKey = null, $localKey = null)
    {
        $instance = $this->newRelatedInstance($related);
        $foreignKey = $foreignKey ?: $this->getForeignKey();
        $localKey = $localKey ?: $this->getKey();

        $query = $instance->newQuery();
        $parent = $this;
        $key = $instance->getTable() . '.' . $foreignKey;

        $query->where($key, $localKey);

        return $query->first();
    }

    public function hasMany($related, $foreignKey = null, $localKey = null)
    {
        $instance = $this->newRelatedInstance($related);
        $foreignKey = $foreignKey ?: $this->getForeignKey();
        $localKey = $localKey ?: $this->getKey();

        $query = $instance->newQuery();
        $parent = $this;
        $key = $instance->getTable() . '.' . $foreignKey;

        $query->where($key, $localKey);

        return $query->get();
    }

    public function belongsTo($related, $foreignKey = null, $ownerKey = null, $relation = null)
    {
        if (is_null($relation)) {
            $relation = $this->guessBelongsToRelation();
        }

        $instance = $this->newRelatedInstance($related);

        if (is_null($foreignKey)) {
            $foreignKey = Str::snake($relation) . '_' . $instance->getKeyName();
        }

        $ownerKey = $ownerKey ?: $instance->getKeyName();

        $query = $instance->newQuery();
        $child = $this;

        $query->where($instance->getTable() . '.' . $ownerKey, '=', $child->{$foreignKey});

        return $query->first();
    }

    public function belongsToMany(
        $related,
        $table = null,
        $foreignPivotKey = null,
        $relatedPivotKey = null,
        $parentKey = null,
        $relatedKey = null,
        $relation = null
    ) {
        if (is_null($relation)) {
            $relation = $this->guessBelongsToManyRelation();
        }

        $instance = $this->newRelatedInstance($related);
        $foreignPivotKey = $foreignPivotKey ?: $this->getForeignKey();
        $relatedPivotKey = $relatedPivotKey ?: $instance->getForeignKey();

        if (is_null($table)) {
            $segments = [
                $instance ? $instance->joiningTableSegment()
                        : Str::snake(class_basename($related)),
                Str::snake(class_basename($this)),
            ];
            sort($segments);
            $table = strtolower(implode('_', $segments));
        }

        $query = $instance->newQuery();
        $parent = $this;
        $parentKey = $parentKey ?: $this->getKeyName();
        $relatedKey = $relatedKey ?: $instance->getKeyName();

        $key = $instance->getTable() . '.' . $relatedKey;
        $query->join($table, $key, '=', $table . '.' . $relatedPivotKey);
        $query->where($table . '.' . $foreignPivotKey, $parent->{$parentKey});

        return $query->get();
    }

    public function joiningTableSegment()
    {
        return Str::snake(class_basename($this));
    }

    protected function newRelatedInstance($class)
    {
        return tap(new $class(), function ($instance) {
            if (! $instance->getConnection()) {
                $instance->setConnection($this->connection);
            }
        });
    }

    protected function guessBelongsToRelation()
    {
        [$one, $two, $caller] = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3);

        return $caller['function'];
    }

    protected function guessBelongsToManyRelation()
    {
        $caller = Arr::first(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS), function ($trace) {
            return ! in_array(
                $trace['function'],
                array_merge(['belongsToMany'], ['guessBelongsToManyRelation'])
            );
        });

        return ! is_null($caller) ? $caller['function'] : null;
    }
}

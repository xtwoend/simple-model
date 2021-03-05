<?php

namespace Xtwoend\Model;

use ArrayAccess;
use Hyperf\DB\DB;
use Carbon\Carbon;
use Hyperf\Utils\Arr;
use Hyperf\Utils\Str;
use JsonSerializable;
use Xtwoend\Model\Builder;
use Xtwoend\Model\Collection;
use Hyperf\Utils\Contracts\Jsonable;
use Hyperf\Utils\Contracts\Arrayable;
use Xtwoend\Model\Query\QueryBuilder;

abstract class Model implements ArrayAccess, Arrayable, Jsonable, JsonSerializable
{
    use Relation;

    protected $connection = 'default';
    protected $table;
    protected $hidden = [];
    protected $attributes = [];
    protected $original = [];
    protected $fillable = [];
    protected $json = [];
    protected $primaryKey = 'id';
    protected static $unguarded = false;
    protected $timestamps = true;
    protected $perPage = 12;
    public $exists = false;
    protected $forceDeleting = true;
    protected $limitHistoryQuery = 5;

    protected $error;

    public function __construct(array $attributes = [])
    {
        $this->syncOriginal();
        $this->fill($attributes);
    }

    public function __call($method, $parameters)
    {
        return call([$this->newQuery(), $method], $parameters);
    }

    public static function __callStatic($method, $parameters)
    {
        return (new static())->{$method}(...$parameters);
    }

    public function newQuery()
    {
        $queryBuilder = (new QueryBuilder(
            DB::connection($this->getConnection()),
            \Hyperf\Utils\Collection::class
        )
        )->setHistoryMax($this->limitHistoryQuery)->table($this->getTable());

        return (new Builder($queryBuilder))->setModel($this);
    }

    public static function query()
    {
        return (new static())->newQuery();
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function setConnection($name)
    {
        $this->connection = $name;
        return $this;
    }

    public function getTable()
    {
        return $this->table ?? Str::snake(Str::pluralStudly(class_basename($this)));
    }

    public function setTable($table)
    {
        $this->table = $table;
        return $this;
    }

    public function getPerPage()
    {
        return $this->perPage;
    }

    public function setPerPage($perPage)
    {
        $this->perPage = $perPege;
        return $this;
    }

    public function getFillable()
    {
        return $this->fillable;
    }

    protected function fillableFromArray(array $attributes)
    {
        if (count($this->getFillable()) > 0 && ! static::$unguarded) {
            return array_intersect_key($attributes, array_flip($this->getFillable()));
        }

        return $attributes;
    }

    public function fill(array $attributes)
    {
        foreach ($this->fillableFromArray($attributes) as $key => $value) {
            $this->setAttribute($key, $value);
        }

        return $this;
    }

    public function forceFill(array $attributes)
    {
        static::$unguarded = true;
        $this->fill($attributes);
        static::$unguarded = false;
        return $this;
    }

    public function setAttribute($key, $value)
    {
        if ($this->hasCast($key)) {
            $value = json_encode($value);
        }
        $this->attributes[$key] = $value;
        return $this;
    }

    public function getAttribute($key)
    {
        if (! $key) {
            return;
        }

        if (array_key_exists($key, $this->getAttributes())) {
            if ($this->hasCast($key)) {
                return $this->castAttribute($key);
            }

            if (in_array($key, ['created_at', 'updated_at'])) {
                return Carbon::parse($this->attributes[$key])->toAtomString() ?? null;
            }

            return $this->attributes[$key] ?? null;
        }

        if (method_exists($this, $key)) {
            $relation = $this->{$key}();
            return $relation;
        }

        return;
    }

    public function castAttribute($key)
    {
        return json_decode($this->attributes[$key]);
    }

    public function getAttributes()
    {
        return $this->attributes;
    }

    public function getHidden()
    {
        return $this->hidden;
    }

    public function setHidden(array $hidden)
    {
        $this->hidden = $hidden;
        return $this;
    }

    public function addHidden($attributes = null)
    {
        $this->hidden = array_merge(
            $this->hidden,
            is_array($attributes) ? $attributes : func_get_args()
        );
    }

    public function syncOriginal()
    {
        $this->original = $this->getAttributes();
        return $this;
    }

    public function isDirty($attributes = null)
    {
        $dirty = $this->getDirty();
        $attributes = is_array($attributes) ? $attributes : func_get_args();

        if (empty($attributes)) {
            return count($dirty) > 0;
        }

        foreach (Arr::wrap($attributes) as $attribute) {
            if (array_key_exists($attribute, $dirty)) {
                return true;
            }
        }

        return false;
    }

    public function getDirty()
    {
        $dirty = [];
        foreach ($this->getAttributes() as $key => $value) {
            if (! $this->originalIsEquivalent($key, $value)) {
                $dirty[$key] = $value;
            }
        }
        return $dirty;
    }

    public function getOriginalAttributes()
    {
        return $this->original;
    }

    public function originalIsEquivalent($key, $current)
    {
        if (! array_key_exists($key, $this->original)) {
            return false;
        }

        $original = Arr::get($this->original, $key);

        if ($current === $original) {
            return true;
        }

        if (is_null($current)) {
            return false;
        }

        return is_numeric($current) && is_numeric($original)
            && strcmp((string) $current, (string) $original) === 0;
    }

    public function __get($key)
    {
        return $this->getAttribute($key);
    }

    public function __set($key, $value)
    {
        $this->setAttribute($key, $value);
        return $this;
    }

    public function __isset($key)
    {
        return $this->offsetExists($key);
    }

    public function __unset($key)
    {
        $this->offsetUnset($key);
    }

    public function getKey()
    {
        return $this->getAttribute($this->getKeyName());
    }

    public function getKeyName()
    {
        return $this->primaryKey;
    }

    public function setKeyName($key)
    {
        $this->primaryKey = $key;
        return $this;
    }

    public function getForeignKey()
    {
        return Str::snake(class_basename($this)) . '_' . $this->getKeyName();
    }

    public static function find($id, $columns = ['*'])
    {
        $instance = new static();
        return $instance->newQuery()->find($id, $columns);
    }

    public function save(): bool
    {
        $saved = false;

        if ($this->timestamps) {
            $this->updateTimestamps();
        }

        if ($this->exists) {
            $saved = $this->isDirty() ? $this->newQuery()
                ->where($this->getKeyName(), $this->getKeyForSaveQuery())
                ->update($this->getAttributes())
                : true;
        } else {
            $id = $this->newQuery()->insertGetId($this->getAttributes());
            $this->setAttribute($this->getKeyName(), $id);
            $saved = $id ?? false;
        }

        return (bool) $saved;
    }

    protected function getKeyForSaveQuery()
    {
        if (isset($this->original[$this->getKeyName()])) {
            return $this->original[$this->getKeyName()];
        } else {
            return $this->getAttribute($this->getKeyName());
        }
    }

    public static function create(array $attributes)
    {
        $instance = new static();
        $instance->forceFill($attributes);
        $instance->save();
        return $instance;
    }

    public function newInstance($attributes = [], $exists = false)
    {
        $model = new static((array) $attributes);
        $model->exists = $exists;
        $model->setConnection($this->getConnection());
        $model->setTable($this->getTable());
        return $model;
    }

    public function newFromBuilder($attributes = [], $connection = null)
    {
        $model = $this->newInstance([], true);
        $model->setRawAttributes((array) $attributes, true);
        $model->setConnection($connection ?: $this->getConnection());
        return $model;
    }

    public function setRawAttributes(array $attributes, $sync = false)
    {
        $this->attributes = $attributes;
        if ($sync) {
            $this->syncOriginal();
        }
        return $this;
    }

    public function refresh()
    {
        $this->attributes = [];
        $this->original = [];

        return $this;
    }

    public function __toString(): string
    {
        return $this->toJson();
    }

    public function toJson($options = 0)
    {
        $json = json_encode($this->toArray(), $options);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error = json_last_error_msg();
        }

        return $json;
    }

    public function jsonSerialize()
    {
        return $this->toArray();
    }

    public function offsetExists($offset)
    {
        return ! is_null($this->getAttribute($offset));
    }

    public function offsetGet($offset)
    {
        return $this->getAttribute($offset);
    }

    public function offsetSet($offset, $value)
    {
        $this->setAttribute($offset, $value);
    }

    public function offsetUnset($offset)
    {
        unset($this->attributes[$offset], $this->relations[$offset]);
    }

    public function toArray(): array
    {
        $attributes = $this->getAttributes();

        foreach ($this->getCasts() as $cast) {
            if (in_array($cast, array_keys($attributes))) {
                $attributes[$cast] = json_decode($attributes[$cast]);
                if ($attributes[$cast] instanceof Arrayable) {
                    $attributes[$cast] = $attributes[$cast]->toArray();
                }
            }
        }

        if (isset($attributes['created_at'])) {
            $attributes['created_at'] = Carbon::parse($attributes['created_at'])->toAtomString();
        }

        if (isset($attributes['updated_at'])) {
            $attributes['updated_at'] = Carbon::parse($attributes['updated_at'])->toAtomString();
        }

        $values = array_diff_key($attributes, array_flip($this->getHidden()));

        return $values;
    }

    public function getError()
    {
        return $this->error;
    }

    public function newCollection(array $models = [])
    {
        return new Collection($models);
    }

    protected function updateTimestamps()
    {
        $time = new Carbon();

        if (! $this->isDirty('updated_at')) {
            $this->setAttribute('updated_at', $time);
        }

        if (! $this->exists and ! $this->isDirty('created_at')) {
            $this->setAttribute('created_at', $time);
        }
    }

    public function hasCast($key)
    {
        return in_array($key, $this->getCasts());
    }

    public function getCasts()
    {
        return $this->json;
    }

    // soft delete
    public function getDeletedAtColumn()
    {
        return defined('static::DELETED_AT') ? static::DELETED_AT : 'deleted_at';
    }

    public function trashed()
    {
        return ! is_null($this->{$this->getDeletedAtColumn()});
    }

    protected function runSoftDelete()
    {
        $query = $this->newQuery()->where($this->getKeyName(), $this->getKey());

        $time = new Carbon();

        $columns = [$this->getDeletedAtColumn() => $time];

        $this->{$this->getDeletedAtColumn()} = $time;

        if ($this->timestamps && ! is_null($this->getAttribute('updated_at'))) {
            $this->updated_at = $time;
            $columns['updated_at'] = $time;
        }

        $query->update($columns);
    }

    public function delete()
    {
        if (! $this->exists) {
            return;
        }

        if ($this->forceDeleting) {
            $this->exists = false;
            return $this->newQuery()->where($this->getKeyName(), $this->getKey())->delete();
        }

        $this->runSoftDelete();

        return true;
    }

    public function restore()
    {
        $this->{$this->getDeletedAtColumn()} = null;
        $this->exists = true;
        return $this->save();
    }

    public function isForceDeleting()
    {
        return $this->forceDeleting;
    }
}

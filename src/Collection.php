<?php

namespace Xtwoend\Model;

use Hyperf\Utils\Arr;
use Xtwoend\Model\Model;
use Hyperf\Utils\Contracts\Arrayable;
use Hyperf\Utils\Collection as BaseCollection;

class Collection extends BaseCollection
{
    /**
     * Find a model in the collection by key.
     *
     * @param null|mixed $default
     * @param mixed $key
     * @return \Xtwoend\Model|static
     */
    public function find($key, $default = null)
    {
        if ($key instanceof Model) {
            $key = $key->getKey();
        }

        if ($key instanceof Arrayable) {
            $key = $key->toArray();
        }

        if (is_array($key)) {
            if ($this->isEmpty()) {
                return new static();
            }

            return $this->whereIn($this->first()->getKeyName(), $key);
        }

        return Arr::first($this->items, function ($model) use ($key) {
            return $model->getKey() === $key;
        }, $default);
    }

    /**
     * Add an item to the collection.
     *
     * @param mixed $item
     * @return $this
     */
    public function add($item)
    {
        $this->items[] = $item;

        return $this;
    }

    /**
     * Determine if a key exists in the collection.
     * @param null|mixed $operator
     * @param null|mixed $value
     * @param mixed $key
     */
    public function contains($key, $operator = null, $value = null): bool
    {
        if (func_num_args() > 1 || $this->useAsCallable($key)) {
            return parent::contains(...func_get_args());
        }

        if ($key instanceof Model) {
            return parent::contains(function ($model) use ($key) {
                return $model->is($key);
            });
        }

        return parent::contains(function ($model) use ($key) {
            return $model->getKey() == $key;
        });
    }

    /**
     * Get the array of primary keys.
     *
     * @return array
     */
    public function modelKeys()
    {
        return array_map(function ($model) {
            return $model->getKey();
        }, $this->items);
    }

    /**
     * Merge the collection with the given items.
     *
     * @param array|\ArrayAccess $items
     * @return static
     */
    public function merge($items): BaseCollection
    {
        $dictionary = $this->getDictionary();

        foreach ($items as $item) {
            $dictionary[$item->getKey()] = $item;
        }

        return new static(array_values($dictionary));
    }

    /**
     * Run a map over each of the items.
     */
    public function map(callable $callback): BaseCollection
    {
        $result = parent::map($callback);

        return $result->contains(function ($item) {
            return ! $item instanceof Model;
        }) ? $result->toBase() : $result;
    }

    /**
     * Reload a fresh model instance from the database for all the entities.
     *
     * @param array|string $with
     * @return static
     */
    public function fresh($with = [])
    {
        if ($this->isEmpty()) {
            return new static();
        }

        $model = $this->first();

        $freshModels = $model->whereIn($model->getKeyName(), $this->modelKeys())
            ->get()
            ->getDictionary();

        return $this->map(function ($model) use ($freshModels) {
            return $model->exists && isset($freshModels[$model->getKey()])
                ? $freshModels[$model->getKey()] : null;
        });
    }

    /**
     * Diff the collection with the given items.
     *
     * @param array|\ArrayAccess $items
     * @return static
     */
    public function diff($items): BaseCollection
    {
        $diff = new static();

        $dictionary = $this->getDictionary($items);

        foreach ($this->items as $item) {
            if (! isset($dictionary[$item->getKey()])) {
                $diff->add($item);
            }
        }

        return $diff;
    }

    /**
     * Intersect the collection with the given items.
     *
     * @param array|\ArrayAccess $items
     * @return static
     */
    public function intersect($items): BaseCollection
    {
        $intersect = new static();

        $dictionary = $this->getDictionary($items);

        foreach ($this->items as $item) {
            if (isset($dictionary[$item->getKey()])) {
                $intersect->add($item);
            }
        }

        return $intersect;
    }

    /**
     * Return only unique items from the collection.
     *
     * @param null|callable|string $key
     */
    public function unique($key = null, bool $strict = false): BaseCollection
    {
        if (! is_null($key)) {
            return parent::unique($key, $strict);
        }

        return new static(array_values($this->getDictionary()));
    }

    /**
     * Returns only the models from the collection with the specified keys.
     *
     * @param mixed $keys
     * @return static
     */
    public function only($keys): BaseCollection
    {
        if (is_null($keys)) {
            return new static($this->items);
        }

        $dictionary = Arr::only($this->getDictionary(), $keys);

        return new static(array_values($dictionary));
    }

    /**
     * Returns only the columns from the collection with the specified keys.
     *
     * @param null|array|string $keys
     */
    public function columns($keys): BaseCollection
    {
        if (is_null($keys)) {
            return new BaseCollection([]);
        }
        $result = [];
        $isSingleColumn = is_string($keys);
        foreach ($this->items as $item) {
            if ($isSingleColumn) {
                $value = $item->{$keys} ?? null;
                $result[] = $value instanceof Arrayable ? $value->toArray() : $value;
            } else {
                $result[] = value(static function () use ($item, $keys) {
                    $res = [];
                    foreach ($keys as $key) {
                        $value = $item->{$key} ?? null;
                        $res[$key] = $value instanceof Arrayable ? $value->toArray() : $value;
                    }

                    return $res;
                });
            }
        }

        return new BaseCollection($result);
    }

    /**
     * Returns all models in the collection except the models with specified keys.
     *
     * @param mixed $keys
     * @return static
     */
    public function except($keys): BaseCollection
    {
        $dictionary = Arr::except($this->getDictionary(), $keys);

        return new static(array_values($dictionary));
    }

    /**
     * Make the given, typically visible, attributes hidden across the entire collection.
     *
     * @param array|string $attributes
     * @return $this
     */
    public function makeHidden($attributes)
    {
        return $this->each->addHidden($attributes);
    }

    /**
     * The following methods are intercepted to always return base collections.
     * @param mixed $value
     */

    /**
     * Get an array with the values of a given key.
     *
     * @param string $value
     */
    public function pluck($value, ?string $key = null): BaseCollection
    {
        return $this->toBase()->pluck($value, $key);
    }

    /**
     * Get the keys of the collection items.
     */
    public function keys(): BaseCollection
    {
        return $this->toBase()->keys();
    }

    /**
     * Zip the collection together with one or more arrays.
     *
     * @param mixed ...$items
     */
    public function zip($items): BaseCollection
    {
        return call_user_func_array([$this->toBase(), 'zip'], func_get_args());
    }

    /**
     * Collapse the collection of items into a single array.
     */
    public function collapse(): BaseCollection
    {
        return $this->toBase()->collapse();
    }

    /**
     * Get a flattened array of the items in the collection.
     * @param float|int $depth
     */
    public function flatten($depth = INF): BaseCollection
    {
        return $this->toBase()->flatten($depth);
    }

    /**
     * Flip the items in the collection.
     */
    public function flip(): BaseCollection
    {
        return $this->toBase()->flip();
    }

    /**
     * Pad collection to the specified length with a value.
     * @param mixed $value
     */
    public function pad(int $size, $value): BaseCollection
    {
        return $this->toBase()->pad($size, $value);
    }

    /**
     * Get a dictionary keyed by primary keys.
     *
     * @param null|array|\ArrayAccess $items
     * @return array
     */
    public function getDictionary($items = null)
    {
        $items = is_null($items) ? $this->items : $items;

        $dictionary = [];

        foreach ($items as $value) {
            $dictionary[$value->getKey()] = $value;
        }

        return $dictionary;
    }
}

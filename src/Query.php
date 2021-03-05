<?php

namespace Xtwoend\Model;

use Hyperf\DB\DB;
use Xtwoend\Model\Query\QueryBuilder;

class Query
{
    protected $query;

    public function __construct(string $poolName = 'default')
    {
        $this->query = new QueryBuilder(DB::connection($poolName));
    }

    public function __call($name, $arguments)
    {
        return call([$this->query, $name], $arguments);
    }

    public static function __callStatic($name, $arguments)
    {
        return (new static())->{$name}(...$arguments);
    }

    public static function connection(string $poolName): self
    {
        return make(static::class, [
            'poolName' => $poolName
        ]);
    }
}

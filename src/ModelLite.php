<?php

namespace Xtwoend\Model;

use Hyperf\DB\DB;
use Hyperf\Utils\Str;
use Xtwoend\Model\Query\QueryBuilder;

abstract class ModelLite 
{
    protected $table;
    protected $connection = 'default';

    public function getTable()
    {
        return $this->table ?? Str::snake(Str::pluralStudly(class_basename($this))); 
    }

    public function setConnection(string $name)
    {
        $this->connection = $name;
        return $this;
    }

    public function query()
    {
        return (new QueryBuilder(DB::connection($this->getConnectionName())))
            ->table($this->getTable());
    }

    public function getConnectionName()
    {
        return $this->connection;
    }

    public function __call($name, $arguments)
    {
        return call([$this->query(), $name], $arguments);
    }

    public static function __callStatic($name, $arguments)
    {
        return (new static())->{$name}(...$arguments);
    }

    public static function connection(string $connection)
    {
        return (new static)->setConnection($connection);
    }
}
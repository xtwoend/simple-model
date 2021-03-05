<?php

namespace Xtwoend\Model\Event;

class QueryExecuted
{
    /**
     * The SQL query that was executed.
     *
     * @var string
     */
    public $sql;

    /**
     * The array of query bindings.
     *
     * @var array
     */
    public $bindings;

    /**
     * The number of milliseconds it took to execute the query.
     *
     * @var float
     */
    public $time;


    /**
     * The result of query.
     *
     * @var null|array|int|\Throwable
     */
    public $result;

    /**
     * Create a new event instance.
     * @param null|array|int|\Throwable $result
     */
    public function __construct(string $sql, array $bindings, ?float $time, $result = null)
    {
        $this->sql = $sql;
        $this->bindings = $bindings;
        $this->time = $time;
        $this->result = $result;
    }
}

<?php

namespace Xtwoend\Model;

use Xtwoend\Model\Model;
use Hyperf\Utils\Contracts\Arrayable;
use Xtwoend\Model\Query\QueryBuilder;

class Builder
{
    protected $query;
    protected $model;
    protected $passthru = [
        'toSql', 'insert', 'insertGetId', 'pluck',
        'count', 'min', 'max', 'avg', 'sum', 'exists',
        'getQueryHistory', 'getLastSql', 'getBindValues',
    ];

    public function __construct(QueryBuilder $query)
    {
        $this->query = $query;
    }

    public function setModel(Model $model)
    {
        $this->model = $model;
        return $this;
    }

    public function getModel()
    {
        return $this->model;
    }

    public function __call($method, $parameters)
    {
        $result = call([$this->query, $method], $parameters);
        return in_array($method, $this->passthru) ? $result : $this;
    }

    public function find($id)
    {
        if (is_array($id) || $id instanceof Arrayable) {
            return $this->findMany($id);
        }

        $this->where($this->model->getKeyName(), '=', $id);

        return $this->first();
    }

    public function findMany($id)
    {
        $this->query->whereIn($this->model->getKeyName(), $id);
        return $this->get();
    }

    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        $this->query->where(...func_get_args());
        return $this;
    }

    public function first()
    {
        return $this->limit(1)->get()->first();
    }

    public function get()
    {
        if (! $this->getModel()->isForceDeleting()) {
            $this->query->whereNull($this->getModel()->getDeletedAtColumn());
        }

        $builder = clone $this;
        $models = $builder->getModels();
        return $builder->getModel()->newCollection($models);
    }

    public function all()
    {
        return $this->get();
    }

    public function paginate(?int $perPage = null, ?int $page = null, array $columns = [])
    {
        $perPage = $perPage ?: $this->model->getPerPage();
        $page = $page ?: request()->input('page', 1);

        $results = ($total = $this->query->getCountForPagination())
            ? $this->select($columns)->forPage($page, $perPage)->get()
            : $this->model->newCollection();

        return (object) [
            'meta' => [
                'total' => (int) $total,
                'page' => (int) $page,
                'per_page' => (int) $perPage
            ],
            'data' => $results
        ];
    }

    public function getModels()
    {
        return $this->model->hydrate(
            $this->query->get()->all()
        )->all();
    }

    public function hydrate(array $items)
    {
        $instance = $this->newModelInstance();
        return $instance->newCollection(array_map(function ($item) use ($instance) {
            return $instance->newFromBuilder($item);
        }, $items));
    }

    public function newModelInstance($attributes = [])
    {
        return $this->model->newInstance($attributes)->setConnection(
            $this->model->getConnection()
        );
    }

    // bonus
    public function allowedSorts($fields)
    {
        if (! class_exists('Xtwoend\\QueryString\\Request')) {
            throw new \RuntimeException("This function require library xtwoend/query-string, please install first.");
        }

        if (is_string($fields)) {
            $fields = func_get_args();
        }
        $sorts = request()->sorts();
        foreach ($sorts as $field => $dir) {
            if (in_array($field, $fields)) {
                $this->query->orderBy($field, $dir);
            }
        }
        return $this;
    }

    public function allowedSearch($fields, $operator = 'equals')
    {
        if (! class_exists('Xtwoend\\QueryString\\Request')) {
            throw new \RuntimeException("This function require library xtwoend/query-string, please install first.");
        }

        if (is_string($fields)) {
            $fields = func_get_args();
            $ch = $fields;
            $operator = array_pop($ch);
            if (in_array($operator, ['contains', 'equals'])) {
                $operator = $operator;
                array_pop($fields);
            }
        }

        $keyword = request()->filter();

        if (! is_null($keyword) && $keyword !== '') {
            foreach ($fields as $field) {
                if ($operator === 'contains') {
                    $this->query->orWhere($field, 'LIKE', "%{$keyword}%");
                } else {
                    $this->query->orWhere($field, $keyword);
                }
            }
        }

        return $this;
    }
}

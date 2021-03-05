# simple model for hyperf nano


## Installation

```
composer require xtwoend/model
```

## Usage

```
use Xtwoend\Model\Model;

class User extends Model
{

}

```

## Fitures

1. Query
2. Sort 
3. Global filter

### sort

```
// ?sort[id]=1&sort[name]=0&q=lorem

$users = User::allowedSorts('id','name)->allowedSearch('name', 'email', 'contains')->panginate();

```


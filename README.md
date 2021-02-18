# grakn_dev_utils

A library of tools to speed up grakn development with the grakn python API:
* initialise and delete test databases
* define a new AttributeType and its owners, or a new RelationType and its role players 
* modify data in a database with a custom function (e.g. add a unique ID)

## Quickstart

### install

with pip:

```
python -m pip install git+https://github.com/JonThom/grakn-dev-utils
```

with pipenv:

```
pipenv install git+https://github.com/JonThom/grakn-dev-utils#egg=grakn_dev_utils
```

use poetry to add to dependencies:

```
poetry add git+https://github.com/JonThom/grakn-dev-utils.git
```

### run

first, make sure grakn 2.0 is installed and running. 

```
import grakn_dev_utils as gradevils
```

initialise a database with default toy schema and data

```
gradevils.init_db("test")
```

delete a database

```
gradevils.del_db("test")
```

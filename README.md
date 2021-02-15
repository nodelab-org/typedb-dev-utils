# grakn_dev_utils

A library of tools to speed up grakn development with the grakn python API:
* initialise and delete test databases
* define a new AttributeType and its owners, or a new RelationType and its role players 
* modify data in a database with a custom function (e.g. add a unique ID)

## Quickstart

### install

with pip:

```
python -m pip install git+https://github.com/JonThom/grakn_dev_utils
```

with pipenv:

```
pipenv install -e git+https://github.com/JonThom/grakn_dev_utils#egg=grakn_dev_utils
```
(The -e argument, for editable, is required for Pipenv to do sub-dependency resolution.)

### run

first, make sure grakn 2.0 is installed and running. 

```
import grakn_dev_utils as gradevils
```

initialise a database with default toy schema and data

```
gradevils.init("test")
```

delete a database

```
gradevils.del("test")
```
# grakn_dev_utils

A library of utilities to speed up grakn development with the grakn python API, including setting up and taking down databases from within Python.


* print types or instances
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

initialise a database and define a schema by reading a gql file line by line
```
gradevils.init_db(
    database="test",
    gql_schema="<path-to-gql.schema>",
    parse_lines=True
)
```

insert data from a gql script, reading line-by-line and modifying each line using a custom function (e.g. to add a unique identifier)
```
def add_UID_to_each_insert_query(line):
    line_modified = line.rstrip("; \n") + ", has UID "  
    quotes = '"' if '"' in line else "'"
    line_modified += quotes + str(uuid.uuid4()) + quotes + ";"
    return line_modified

gradevils.insert_data(
    database="test",
    gql_data=<path-to-gql.data>,
    parse_lines=True,
    line_modifier=add_UID_to_each_insert_query,
    verbose=True)
```

delete a database
```
gradevils.del_db(database="test")
```

see the first 5 EntityTypes, RelationTypes and AttributeTypes	
```
gradevils.ls_types(
    database="test",
    n=5,
    thingtypes=["entity","relation","attribute"])
```

see the top 5 EntityTypes, RelationTypes and AttributeTypes, including their attributes and relations	
```
gradevils.ls_instances(
    database="test",
    n=5
    thingtypes=["entity","relation","attribute"],
    print_attributes = True,
    print_relations = True)
```

define a new AttributeType and make all persons and companies own it
```
gradevils.def_attr_type(
    database="test",
    new_attr_label="name",
    new_attr_value="string",
    sup_label="attribute",
    is_key=False,
    thingtypes = ["person", "company"])
```

define a new RelationType that inherits from the root RelationType
```
gradevils.def_rel_type(
    database="test",
    new_rel_label="crime-syndicate",
    dict_role_players={"ring-leader":{"role_players":["person"], "role_sup":"role"}, {"grunt":{"role_players":["person"], "role_sup":"role"}},
    rel_sup="relation")
```

get the AttributeTypes owned by a ThingType
```
gradevils.get_type_owns(
    database="test",
    thingtype="person")
```

get the RelationTypes in which ThingType is a role player
```
gradevils.get_type_plays(
    database="test",
    thingtype="person")
```

Check out the function docstrings for details

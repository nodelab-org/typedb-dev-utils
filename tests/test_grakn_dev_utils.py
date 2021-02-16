import pytest

import grakn_dev_utils as gradevils 

#test 

#variables
database="test"
gql_schema="/Users/rkm916/Sync/data/2020-testdata/tenancy_schema.gql"
gql_data="/Users/rkm916/Sync/data/2020-testdata/tenancy_data.gql"

# del db
del_db(database)

init_db_schema(database, gql_schema, gql_data, parse_lines=False, parse_lines=True)

#test 
del_db(database)
init_db(database, gql_schema, gql_data, parse_lines=False, parse_lines=True)
def_attr_type(
    database, 
    new_attr_label="UID", 
    new_attr_value="string", 
    sup_label="attribute",
    is_key=True,
    thingtypes = ["entity", "relation", "attribute"], 
    host="localhost", 
    port="1729")

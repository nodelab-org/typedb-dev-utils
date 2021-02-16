import pytest
import grakn_dev_utils as gradevils

#test 

#variables
database="test"
gql_schema="/Users/rkm916/Sync/data/2020-testdata/tenancy_schema.gql"
gql_data="/Users/rkm916/Sync/data/2020-testdata/tenancy_data.gql"

#test 
gradevils.del_db(database)
gradevils.init_db(database, gql_schema, parse_lines=False)
gradevils.def_attr_type(database, new_attr_label="UID",  new_attr_value="string", sup_label="attribute", is_key=True, thingtypes = ["entity", "relation", "attribute"])
gradevils.ls_types(database)
gradevils.get_owns(database, "person")

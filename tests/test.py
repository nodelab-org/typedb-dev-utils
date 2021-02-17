import pytest
import grakn_dev_utils as gradevils
import uuid


#variables
database="test"
gql_schema="/Users/rkm916/Sync/data/2020-testdata/tenancy_schema.gql"
gql_data="/Users/rkm916/Sync/data/2020-testdata/tenancy_data.gql"
verbose=True 

#test 
gradevils.del_db(database)
gradevils.init_db(database, gql_schema, parse_lines=False, verbose=verbose)
gradevils.def_attr_type(database, new_attr_label="UID",  new_attr_value="string", sup_label="attribute", is_key=True, thingtypes = ["entity", "relation"], verbose=verbose)
gradevils.get_type_owns(database, "person")
gradevils.ls_types(database)
gradevils.def_rel_type(database, "association", dict_role_players={"associate":["house","person"]}, sup="relation", verbose=verbose)
gradevils.get_type_plays(database, "person")

def add_UID_to_each_insert_query(line):
    #print(line)
    line_modified = line.rstrip("; \n") + ", has UID "  
    quotes = '"' if '"' in line else "'"
    line_modified += quotes + str(uuid.uuid4()) + quotes + ";"
    # print("this is the modified line:")
    # print(line_modified)
    return line_modified

gradevils.insert_data(database, gql_data, parse_lines=True, line_modifier=add_UID_to_each_insert_query, verbose=verbose)

gradevils.ls_instances(database,5,["entity","relation"], True, True)

def thing_modifier_test(write_transaction, thing):
    print(thing.as_remote(write_transaction).get_type().get_label())

modify_things(
    database, 
    query_match = "match $x isa thing; get $x;", 
    thing_modifier = thing_modifier_test,
    args=None,
    verbose=False,
    host="localhost", 
    port="1729")
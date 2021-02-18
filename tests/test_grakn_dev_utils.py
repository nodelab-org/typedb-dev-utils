#from typing import type_check_only
import grakn_dev_utils as gradevils
from grakn.client import *
import uuid
import os 

# variables
database="test"
gql_schema="/Users/rkm916/Sync/data/2020-testdata/tenancy_schema.gql"
gql_data="/Users/rkm916/Sync/data/2020-testdata/tenancy_data.gql"
host="localhost"
port="1729"
verbose=True 

# start grakn server
#os.system("grakn server")

# tests
def test_init_db_1():
    gradevils.del_db(database)    
    gradevils.init_db(database,gql_schema=gql_schema, parse_lines=False)    
    with GraknClient.core(host+":"+port) as client:
        assert client.databases().contains(database)


def test_init_db_2():
    gradevils.del_db(database)    
    gradevils.init_db(database, gql_schema, parse_lines=False)    
    
    query_match_entity = "match $x sub entity; get $x;"
    query_match_relation = "match $x sub relation; get $x;"
    query_match_attribute = "match $x sub attribute; get $x;"
    
    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap_entity = read_transaction.query().match(query_match_entity)
                list_concept_entity = [conceptMap_entity.get("x") for conceptMap_entity in iterator_conceptMap_entity]

                iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
                list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

                iterator_conceptMap_attribute = read_transaction.query().match(query_match_attribute)
                list_concept_attribute = [conceptMap_attribute.get("x") for conceptMap_attribute in iterator_conceptMap_attribute]

                assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_relation, list_concept_attribute]])


def test_del_db():
    gradevils.del_db(database)
    with GraknClient.core(host+":"+port) as client:
        assert not client.databases().contains(database)


def test_ls_types():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)
    
    gradevils.ls_types(database)
    assert True    


def test_def_attr_type_and_get_type_owns():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)

    gradevils.def_attr_type(database, new_attr_label = "test_attr", new_attr_value="string", sup_label="attribute", is_key=False, thingtypes=["person"])
    #gradevils.def_attr_type(database, new_attr_label = "test_attr_child", new_attr_value="string", sup_label="test_attr", is_key=False, thingtypes=["person"])
    assert all([attr in gradevils.get_type_owns(database, "person") for attr in ["test_attr"]])

def test_def_rel_type():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)
    
    gradevils.def_rel_type(database, "association", dict_role_players={"associate":{"role_players":["house","person"], "role_sup":"role"}}, rel_sup="relation", verbose=verbose)
    gradevils.def_rel_type(database, "association_sub", dict_role_players={"sub_role":{"role_players":["house","person"], "role_sup":"associate"}}, rel_sup="association", verbose=verbose)

    person_scoped_roles = gradevils.get_type_plays(database, thingtype="person")
    assert any([lambda scoped_role: "associate" in scoped_role,person_scoped_roles]) and any([lambda scoped_role: "sub_role" in scoped_role,person_scoped_roles]) 


def test_insert_data():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)
    
    gradevils.def_attr_type(database, new_attr_label = "UID", new_attr_value="string", sup_label="attribute", is_key=False, thingtypes=["entity","relation","attribute"])
    
    def add_UID_to_each_insert_query(line):
        line_modified = line.rstrip("; \n") + ", has UID "  
        quotes = '"' if '"' in line else "'"
        line_modified += quotes + str(uuid.uuid4()) + quotes + ";"
        return line_modified
        
    gradevils.insert_data(database, gql_data, parse_lines=True, line_modifier = add_UID_to_each_insert_query)

    query_match_entity = "match $x isa entity; $x has UID $attr_val; get $x, $attr_val;"
    query_match_relation = "match $x isa relation; get $x;"
    
    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.DATA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap_entity = read_transaction.query().match(query_match_entity) 
                list_dict_conceptMap = [conceptMap_entity.map() for conceptMap_entity in iterator_conceptMap_entity]
                list_concept_entity = [dict_conceptMap["x"] for dict_conceptMap in list_dict_conceptMap]
                list_concept_entity_attr_val = [dict_conceptMap["attr_val"] for dict_conceptMap in list_dict_conceptMap]

                iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
                list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

                assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_entity_attr_val, list_concept_relation]])


def test_ls_instances():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)
    gradevils.insert_data(database, gql_data, parse_lines=True)

    gradevils.ls_instances(database)
    assert True    


def test_modify_things():
    gradevils.del_db(database)
    gradevils.init_db(database, gql_schema, parse_lines=False)

    def thing_modifier_test(write_transaction, thing):
        print(thing.as_remote(write_transaction).get_type().get_label())

    gradevils.modify_things(
        database, 
        query_match = "match $x isa thing; get $x;", 
        thing_modifier = thing_modifier_test,
        args=None,
        host="localhost", 
        port="1729")
    
    assert True 
#from typing import type_check_only
import grakn_dev_utils as gradevils
from grakn.client import *
import uuid
from .fixtures import *

# variables

# tests
def test_init_db_1(database_params, db_client):    
    assert db_client.databases().contains(database_params["database"])

def test_init_db_2(database_params, db_client):

    query_match_entity = "match $x sub entity; get $x;"
    query_match_relation = "match $x sub relation; get $x;"
    query_match_attribute = "match $x sub attribute; get $x;"

    with db_client.session(database_params["database"], SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap_entity = read_transaction.query().match(query_match_entity)
            list_concept_entity = [conceptMap_entity.get("x") for conceptMap_entity in iterator_conceptMap_entity]

            iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
            list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

            iterator_conceptMap_attribute = read_transaction.query().match(query_match_attribute)
            list_concept_attribute = [conceptMap_attribute.get("x") for conceptMap_attribute in iterator_conceptMap_attribute]

            assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_relation, list_concept_attribute]])


def test_ls_types(database_params, db_client):
    db_client.close()
    gradevils.ls_types(database_params["database"])
    assert True

# TODO FAILS
def test_def_attr_type_and_get_type_owns(database_params, db_client):
    db_client.close()
    gradevils.def_attr_type(database_params["database"], new_attr_label = "test_attr", new_attr_value="string", sup_label="attribute", is_key=False, thingtypes=["person"], verbose=True)
    #gradevils.def_attr_type(database, new_attr_label = "test_attr_child", new_attr_value="string", sup_label="test_attr", is_key=False, thingtypes=["person"])
    print('gradevils.get_type_owns(database_params["database"], "person")')
    print(gradevils.get_type_owns(database_params["database"], "person"))
    assert all([attr in gradevils.get_type_owns(database_params["database"], "person") for attr in ["test_attr"]])


def test_def_rel_type(database_params, db_client):
    db_client.close()
    gradevils.def_rel_type(database_params["database"], "association", dict_role_players={"associate":{"role_players":["house","person"], "role_sup":"role"}}, rel_sup="relation", verbose=True)
    gradevils.def_rel_type(database_params["database"], "association_sub", dict_role_players={"sub_role":{"role_players":["house","person"], "role_sup":"associate"}}, rel_sup="association", verbose=True)

    person_scoped_roles = gradevils.get_type_plays(database_params["database"], thingtype="person")
    assert any([lambda scoped_role: "associate" in scoped_role,person_scoped_roles]) and any([lambda scoped_role: "sub_role" in scoped_role,person_scoped_roles])


def test_insert_data(database_params, db_client):
    db_client.close()
    gradevils.def_attr_type(database_params["database"], new_attr_label = "UID", new_attr_value="string", sup_label="attribute", is_key=False, thingtypes=["entity","relation","attribute"])

    def add_UID_to_each_insert_query(line):
        line_modified = line.rstrip("; \n") + ", has UID "
        quotes = '"' if '"' in line else "'"
        line_modified += quotes + str(uuid.uuid4()) + quotes + ";"
        return line_modified

    gradevils.insert_data(database_params["database"], database_params["gql_data"], parse_lines=True, line_modifier = add_UID_to_each_insert_query)

    query_match_entity = "match $x isa entity; $x has UID $attr_val; get $x, $attr_val;"
    query_match_relation = "match $x isa relation; get $x;"

    with GraknClient.core(database_params["host"]+":"+database_params["port"]) as client:
        with client.session(database_params["database"], SessionType.DATA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap_entity = read_transaction.query().match(query_match_entity)
                list_dict_conceptMap = [conceptMap_entity.map() for conceptMap_entity in iterator_conceptMap_entity]
                list_concept_entity = [dict_conceptMap["x"] for dict_conceptMap in list_dict_conceptMap]
                list_concept_entity_attr_val = [dict_conceptMap["attr_val"] for dict_conceptMap in list_dict_conceptMap]

                iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
                list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

                assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_entity_attr_val, list_concept_relation]])


def test_ls_instances(database_params, db_client):
    db_client.close()
    gradevils.insert_data(database_params["database"], database_params["gql_data"], parse_lines=True)
    gradevils.ls_instances(database_params["database"])
    assert True


def test_modify_things(database_params,db_client):
    db_client.close()
    def thing_modifier_test(write_transaction, thing):
        print(thing.get_type().get_label())

    gradevils.modify_things(
        database_params["database"],
        query_match = "match $x isa thing; get $x;",
        thing_modifier = thing_modifier_test,
        args=None,
        host=database_params["host"],
        port=database_params["port"])

    assert True

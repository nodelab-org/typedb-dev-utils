import typedb_dev_utils as gradevils
from typedb.client import *
import uuid
from .fixtures import *

def test_init_db_1(database_params, db_client_core):
    assert db_client_core.databases().contains(database_params["database"])

def test_init_db_2(database_params, db_client_core):

    query_match_entity = "match $x sub entity; get $x;"
    query_match_relation = "match $x sub relation; get $x;"
    query_match_attribute = "match $x sub attribute; get $x;"

    with db_client_core.session(database_params["database"], SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap_entity = read_transaction.query().match(query_match_entity)
            list_concept_entity = [conceptMap_entity.get("x") for conceptMap_entity in iterator_conceptMap_entity]

            iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
            list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

            iterator_conceptMap_attribute = read_transaction.query().match(query_match_attribute)
            list_concept_attribute = [conceptMap_attribute.get("x") for conceptMap_attribute in iterator_conceptMap_attribute]

            assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_relation, list_concept_attribute]])


def test_ls_types(database_params, db_client_core):
    gradevils.ls_types(database_params["database"], client=db_client_core)
    assert True

def test_def_attr_type_and_get_type_owns(database_params, db_client_core):
    gradevils.def_attr_type(database=database_params["database"], new_attr_label = "test_attr", new_attr_value="string", sup_label="attribute", is_key=False, thingTypes=["person"], verbose=True, client=db_client_core)
    assert all([attr in gradevils.get_type_owns(database_params["database"], thingType = "person", client=db_client_core) for attr in ["test_attr"]])


def test_def_rel_type(database_params, db_client_core):
    client = gradevils.def_rel_type(database_params["database"], "association", dict_role_players={"associate":{"role_players":["house","person"], "role_sup":"role"}}, rel_sup="relation", verbose=True, client=db_client_core, return_client = True)
    client =gradevils.def_rel_type(database_params["database"], "association_sub", dict_role_players={"sub_role":{"role_players":["house","person"], "role_sup":"associate"}}, rel_sup="association", verbose=True, client=client, return_client=True)

    person_scoped_roles = gradevils.get_type_plays(database_params["database"], thingType="person", client=client)
    assert any([lambda scoped_role: "associate" in scoped_role,person_scoped_roles]) and any([lambda scoped_role: "sub_role" in scoped_role,person_scoped_roles])


def test_insert_data(database_params, db_client_core):
    client = gradevils.def_attr_type(database_params["database"], new_attr_label = "UID", new_attr_value="string", sup_label="attribute", is_key=False, rootTypes=["entity","relation","attribute"], client=db_client_core, return_client = True)

    def add_UID_to_each_insert_query(line):
        line_modified = line.rstrip("; \n") + ", has UID "
        quotes = '"' if '"' in line else "'"
        line_modified += quotes + str(uuid.uuid4()) + quotes + ";"
        return line_modified

    client = gradevils.insert_data(database_params["database"], database_params["gql_data"], parse_lines=True, line_modifier = add_UID_to_each_insert_query, client=client, return_client = True)

    query_match_entity = "match $x isa entity; $x has UID $attr_val; get $x, $attr_val;"
    query_match_relation = "match $x isa relation; get $x;"

    with client.session(database_params["database"], SessionType.DATA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap_entity = read_transaction.query().match(query_match_entity)
            list_dict_conceptMap = [conceptMap_entity.map() for conceptMap_entity in iterator_conceptMap_entity]
            list_concept_entity = [dict_conceptMap["x"] for dict_conceptMap in list_dict_conceptMap]
            list_concept_entity_attr_val = [dict_conceptMap["attr_val"] for dict_conceptMap in list_dict_conceptMap]

            iterator_conceptMap_relation = read_transaction.query().match(query_match_relation)
            list_concept_relation = [conceptMap_relation.get("x") for conceptMap_relation in iterator_conceptMap_relation]

            assert all([lambda list_concept: len(list_concept)>1, [list_concept_entity, list_concept_entity_attr_val, list_concept_relation]])


def test_ls_instances(database_params, db_client_core):
    client = gradevils.insert_data(database_params["database"], database_params["gql_data"], parse_lines=True, client=db_client_core, return_client = True)
    gradevils.ls_instances(database_params["database"], client=client)
    assert True


def test_modify_each_concept_1(database_params, db_client_core):
    new_attr_label = "identifier"
    db_client_core = gradevils.insert_data(database_params["database"], database_params["gql_data"], parse_lines=True, client=db_client_core, return_client = True)
    # modify schema: add new attribute to person entitytype
    db_client_core = gradevils.def_attr_type(
        database = database_params["database"],
        new_attr_label=new_attr_label,
        new_attr_value="string",
        sup_label="attribute",
        is_key=False,
        thingTypes = ["person","house"],
        rootTypes = None,
        verbose=False,
        client=db_client_core,
        return_client=True,
        host="localhost",
        port="1729",
        parallelisation=4)

    query_match =  "match $x isa person; "

    def f_write_test(write_transaction, iid, new_attr_label):
        query = "match $x iid {0}; insert $x has {1} '{2}';".format(iid, new_attr_label, str(uuid.uuid4()))
        print("write query: {}".format(query))
        write_transaction.query().insert(query)
        return None

    db_client_core = gradevils.modify_each_concept(
        database = database_params["database"],
        query_match = query_match,
        f_write = f_write_test,
        args=[new_attr_label],
        host=database_params["host"],
        port=database_params["port"],
        client=db_client_core,
        return_client=True,
        batch_size=2)

    # verify that the instances have been changes
    with db_client_core.session(database_params["database"], SessionType.DATA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap = read_transaction.query().match("match $x isa person, has identifier $y; get $y; ")
            list_dict_concept = [conceptMap.map() for conceptMap in iterator_conceptMap]
            assert len(list_dict_concept)
            for dict_concept in list_dict_concept:
                attr_val = dict_concept["y"].get_value()
                print(attr_val)
                assert type(attr_val) is str and len(attr_val)

from typedb.client import *
import py_dev_utils
#import itertools


def del_db(
    database,
    cluster = False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    client=None,
    return_client=False,
    parallelisation=2,
    verbose=False
    ):
    '''@usage delete a typedb database
    @param database the database to delete, string
    @param cluster: boolean, whether it is typedb cluster
    @param host the host for typedb core, string
    @param port the port for typedb core, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    @param verbose: whether to print databases after deletion, bool
    @return None
    '''
    if client is None:
        if cluster: 
            credential = TypeDBCredential(
                username=username, 
                password=password, 
                tls_root_ca_path=tls_root_ca_path)
            TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
        else:
            client = TypeDB.core_client(address=host+":"+port, parallelisation=parallelisation)
    if client.databases().contains(database):
        client.databases().get(database).delete()
    else:
        print(database + " not found")
    if verbose:
        print("deleted " + database)
        print("databases: {}".format([db.name() for db in client.databases().all()]))

    if return_client:
        return client
    else:
        client.close
        return None


def init_db(
    database,
    cluster = False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    gql_schema=None,
    parse_lines=False,
    client=None,
    return_client=False,
    parallelisation=2,
    verbose=False
    ):
    '''
    @param database: the database to intialise, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param gql_schema: path to schema, string
    @param parse_lines: whether to parse gql_schema line-by-line or as a whole. Bool, default False
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    @param parallelisation: passed to TypeDB client
    @param verbose: if True, print the define queries
    @return None 
    '''
    if client is None:
        if cluster: 
            credential = TypeDBCredential(
                username=username, 
                password=password, 
                tls_root_ca_path=tls_root_ca_path)
            TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
        else:
            client = TypeDB.core_client(address=host+":"+port, parallelisation=parallelisation)
    client.databases().create(database)
    if not gql_schema is None:
        if parse_lines:
            f = open(gql_schema, "r")#
            with client.session(database, SessionType.SCHEMA) as session:
                for line in f.readlines():
                    if all([token in line for token in ["define","sub",";"]]):
                        if verbose:
                            print(line)
                        with session.transaction(TransactionType.WRITE) as write_transaction:
                            write_transaction.query().define(line)
                            write_transaction.commit()
        else:
            query_define = open(gql_schema, "r").read()
            if verbose:
                print(query_define)
            with client.session(database, SessionType.SCHEMA) as session:
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    write_transaction.query().define(query_define)
                    write_transaction.commit()
    if verbose:
        print("initiated " + database)
        print("databases: {}".format([db.name() for db in client.databases().all()]))

    if return_client:
        return client
    else:
        client.close
        return None



def ls_types(
    database,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    n=float("inf"),
    rootTypes=["entity","relation","attribute"],
    client=None,
    return_client=False,
    parallelisation=2
    ):
    '''@usage print the types in a schema. Useful for getting a peak into the schema.
    @param database: the database to intialise, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param n: the max number of each root type to print, default all
    @param rootTypes: the root types for which to print subtypes
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    '''

    list_query_match = ["match $x sub {}; get $x;".format(rootType) for rootType in rootTypes]

    if client is None:
        if cluster: 
            credential = TypeDBCredential(
                username=username, 
                password=password, 
                tls_root_ca_path=tls_root_ca_path)
            TypeDB.cluster_client(
                addresses = addresses,
                credential = credential,
                parallelisation=parallelisation)
        else:
            client = TypeDB.core_client(address=host+":"+port, parallelisation=parallelisation)
    with client.session(database, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            for i in range(len(list_query_match)):
                query_match = list_query_match[i]
                iterator_conceptMap = read_transaction.query().match(query_match)
                k=0
                print("===============")
                print(rootTypes[i].upper())
                print("===============")
                for conceptMap in iterator_conceptMap:
                    if not conceptMap.get("x").get_label().name() in ["entity", "relation", "attribute"]:
                        print(conceptMap.get("x").get_label().name())
                        k+=1
                        if k==n:
                            break
    if return_client:
        return client
    else:
        client.close
        return None


def def_attr_type(
    database,
    new_attr_label,
    new_attr_value,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    sup_label="attribute",
    is_key=False,
    thingTypes = None,
    rootTypes = None,
    verbose=False,
    client=None,
    return_client=False,
    parallelisation=2):
    '''@usage: add a new attribute to all or subset of thingTypes
    @param database: the name of the database. string
    @param new_attr_label: the label of the new attribute. string
    @param new_attr_value: the value type of the new attribute, one of "long", "double", "string", "boolean" or "datetime". string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param sup_label: the attribute supertype  which the new attributetype will inherit
    @param is_key: is the attribute a key, bool
    @param thingTypes: list of thingTypes which, with their subtypes, will own the attribute. if rootTypes is provided will be ignored
    @param rootTypes: list of rootTypes whose subtypes will own the attribute. Trumps thingTypes
    @param verbose: if True, print the define queries
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    @return None
    '''
    if not thingTypes is None and not rootTypes is None:
        raise ValueError("One and only one of thingTypes or rootTypes must be provided")
    if thingTypes is None and rootTypes is None:
        raise ValueError("One of thingTypes or rootTypes must be provided")

    if not thingTypes is None:
        list_query_match = ["match $x type {}; get $x;".format(thingType) for thingType in thingTypes]
    elif not rootTypes is None:
        list_query_match = ["match $x sub! {}; get $x;".format(rootType) for rootType in rootTypes]
    query_define_attr = "define {0} sub {1}, value {2};".format(new_attr_label, sup_label, new_attr_value)
    list_concept = []

    # get all the types in the schema
    if client is None:
        if cluster: 
            credential = TypeDBCredential(
                username=username, 
                password=password, 
                tls_root_ca_path=tls_root_ca_path)
            TypeDB.cluster_client(
                addresses = addresses,
                credential = credential,
                parallelisation=parallelisation)
        else:
            client = TypeDB.core_client(
                address=host+":"+port, 
                parallelisation=parallelisation)
    with client.session(database, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            for query_match in list_query_match:
                iterator_conceptMap = read_transaction.query().match(query_match)
                for conceptMap in iterator_conceptMap:
                    if not conceptMap.get("x").get_label().name() in ["entity", "relation", "attribute"]:
                        list_concept.append(conceptMap.get("x"))

        # define the new attribute
        with session.transaction(TransactionType.WRITE) as write_transaction:
            if verbose:
                print(query_define_attr)
            write_transaction.query().define(query_define_attr)
            write_transaction.commit()

        # make existing types own the new attribute
        for concept in list_concept:
            with session.transaction(TransactionType.READ) as read_transaction:
                concept_sup_label = concept.as_remote(read_transaction).get_supertype().get_label().name()
            with session.transaction(TransactionType.WRITE) as write_transaction:
                query_define_owns = "define {0} sub {1},".format(concept.get_label().name(), concept_sup_label)
                if concept.is_attribute_type():
                    valuetype = str(concept.get_value_type()).split(".")[1].lower()
                    query_define_owns += "value " + valuetype + ", "
                query_define_owns += "owns {}".format(new_attr_label)
                if is_key:
                    query_define_owns += " @key"
                query_define_owns += ";"
                if verbose:
                    print(query_define_owns)
                write_transaction.query().define(query_define_owns)
                write_transaction.commit()
    if return_client:
        return client
    else:
        client.close
        return None


def get_type_owns(
    database,
    thingType,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    client=None,
    parallelisation=2
    ):
    '''@usage get the attribute types owned by thingType
    @param database: the database, string
    @param thingType: the thingType for which to retrieve attributes, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param client: an active typedb client
    @return dict of string {"attr1":valuetype, "attr2":valuetype, ... "@key":"attr1"}
            where the "@key" key returns the name of the key attribute (if it exists)
    '''
    
    query_thingType = "match $x type {}; get $x;".format(thingType)
    dict_out = {}

    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    with client.session(database, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap = read_transaction.query().match(query_thingType)
            concept = next(iterator_conceptMap).get("x")
            iterator_attr = concept.as_remote(read_transaction).get_owns(value_type=None, keys_only=False)
            for attrtype in iterator_attr:
                dict_out[attrtype.get_label().name()] = str(attrtype.get_value_type()).split(".")[1].lower()
            iterator_key = concept.as_remote(read_transaction).get_owns(value_type=None, keys_only=True)
            iterator_key = py_dev_utils.check_whether_iterator_empty(iterator_key)
            if not iterator_key is None:
                dict_out["@key"] = next(iterator_key).get_label().name()
    client.close()
    return dict_out


def def_rel_type(
    database,
    new_rel_label,
    dict_role_players,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    rel_sup="relation",
    verbose=False,
    client=None,
    return_client=False,
    parallelisation=2
    ):
    '''@usage: add a new relationtype to the schema
    @param database: the name of the database. string
    @param new_rel_label: the label of the new relation type. string
    @param dict_role_players: dict
            keys: role labels (string)
            values: dict
                key-values:
                    "role_players": array of role_player types (string)
                    "role_sup": role supertype label ("role" if inheriting from root Role)
           to make a role applicable to all descendents of one or more root types, provide one or more of the root type(s) ["entity", "relation", "attribute"]
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param rel_sup: the supertype form which the new relationtype will inherit
    @param verbose: if True, print the define queries
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    @return None
    '''

    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    with client.session(database, SessionType.SCHEMA) as session:
        # check if any root types included
        for role_label in dict_role_players.keys():
            list_role_players = dict_role_players[role_label]["role_players"]
            role_sup_label = dict_role_players[role_label]["role_sup"]
            for root_type in ["entity", "relation", "attribute"]:
                if root_type in list_role_players:
                    with session.transaction(TransactionType.READ) as read_transaction:
                        query_sub = "match $x sub {}; get $x;".format(root_type)
                        iterator_conceptMap = read_transaction.query().match(query_sub)
                        for conceptMap in iterator_conceptMap:
                            dict_role_players[role_label]["role_players"].append(conceptMap.get("x").get_label().name())
                    # remove the root type from the role players
                    idx = dict_role_players[role_label]["role_players"].index(root_type)
                    dict_role_players[role_label]["role_players"].pop(idx)

        # prepare define relation query
        query_define_rel = "define {0} sub {1}, ".format(new_rel_label, rel_sup)
        list_clause_relates = ["relates {} as {}".format(role, dict_role_players[role]["role_sup"]) for role in dict_role_players.keys()]
        query_define_rel += ", ".join(list_clause_relates) + ";"

        with session.transaction(TransactionType.WRITE) as write_transaction:
            # define relation type
            if verbose:
                print(query_define_rel)
            write_transaction.query().define(query_define_rel)
            write_transaction.commit()

        # add "plays" to existing types
        for role_label, dict_role_player in dict_role_players.items():
            for role_player_label in dict_role_player["role_players"]:
                # get sup
                with session.transaction(TransactionType.READ) as read_transaction:
                    role_player_concept = read_transaction.concepts().get_thing_type(role_player_label)
                    rp_sup_label = role_player_concept.as_remote(read_transaction).get_supertype().get_label().name()
                query_define_plays = "define {0} sub {1}, plays {2}:{3};".format(role_player_label, rp_sup_label, new_rel_label, role_label)
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    if verbose:
                        print(query_define_plays)
                    write_transaction.query().define(query_define_plays)
                    write_transaction.commit()
    if return_client:
        return client
    else:
        client.close
        return None

def get_type_plays(
    database,
    thingType,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    client=None,
    parallelisation=2
    ):
    '''@usage get the roles played by thingType
    @param database: the database, string
    @param thingType: the thingType for which to retrieve attributes, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param client: an active typedb client
    @return list of string ["rel1:role1", "rel1:role2", "rel2:role3"..]
    '''
    query_thingType = "match $x type {}; get $x;".format(thingType)
    list_out = []


    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    with client.session(database, SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap = read_transaction.query().match(query_thingType)
            concept = next(iterator_conceptMap).get("x")
            iterator_roletype = concept.as_remote(read_transaction).get_plays()
            for roletype in iterator_roletype:
                list_out.append(roletype.get_label().scoped_name())
    client.close()

    # print("list_out")
    # print(list_out)

    list_out.sort()
    return list_out


def insert_data(
    database,
    gql_data,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    parse_lines=False,
    line_modifier = lambda line: line,
    verbose=False,
    client=None,
    return_client=False,
    parallelisation=2):
    '''
    @param database: the database to intialise, string
    @param gql_data: path to data, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param parse_lines: whether to parse gql_data line-by-line or as a whole. bool, default False
    @param verbose: if True, print the insert queries
    @param line_modifier: if parse_lines, optionally pre-process each line using a provided function that takes a string input and returns the modified line.
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    '''

    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    if parse_lines:
        f = open(gql_data, "r")
        with client.session(database, SessionType.DATA) as session:
            for line in f.readlines():
                if all([token in line for token in ["insert",";"]]) and not line.rstrip()[0]=="#":
                    line = line_modifier(line)
                    if verbose:
                        print(line)
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        write_transaction.query().insert(line)
                        write_transaction.commit()
    else:
        query_insert = open(gql_data, "r").read()
        if verbose:
            print(query_insert)
        with client.session(database, SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                write_transaction.query().insert(query_insert)
                write_transaction.commit()
    if verbose:
        print("databases: {}".format(client.databases().all()))
    if return_client:
        return client
    else:
        client.close
        return None


def ls_instances(
    database,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    n=10,
    thingTypes=["entity","relation","attribute"],
    print_attributes = True,
    print_relations = True,
    client=None,
    return_client=False,
    parallelisation=2):
    '''@usage print the top n instances of each root type, along with an attribute and a relation.
              useful for getting a peak into the data
    @param database: the database to intialise, string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param n: the max number of each type to print, default all
    @param thingTypes: the types for which to print subtypes
    @param print_attributes: print the attributes owned by the type, if any
    @param print_relations: print the relations in which instance plays a role, if any
    @param client: an active typedb client
    @param return_client: return typedb client, if not, client is closed
    '''

    list_query_match = ["match $x isa {}; ".format(thingType) for thingType in thingTypes]
    get_clause = "get $x"
    if print_attributes:
        list_query_match = [query_match + "$x has attribute $attr; " for query_match in list_query_match]
        get_clause += ", $attr"
    if print_relations:
        list_query_match = [query_match + "$rel ($role:$x) isa relation; " for query_match in list_query_match]
        get_clause += ", $rel, $role"
    get_clause += ";"
    list_query_match = [query_match + get_clause for query_match in list_query_match]


    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    with client.session(database, SessionType.DATA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            for i in range(len(list_query_match)):
                query_match = list_query_match[i]
                iterator_conceptMap = read_transaction.query().match(query_match)
                iterator_conceptMap = py_dev_utils.check_whether_iterator_empty(iterator_conceptMap)
                if not iterator_conceptMap is None:
                    k=0
                    print("===============")
                    print(thingTypes[i].upper())
                    print("===============")
                    for conceptMap in iterator_conceptMap:
                        dict_concept = conceptMap.map()
                        concept = dict_concept["x"]
                        iid = concept.get_iid()
                        type_label = concept.get_type().get_label().name()
                        line_print = "$x iid {0} isa {1}; ".format(iid, type_label)
                        if print_attributes:
                            concept_attr = dict_concept["attr"]
                            attr_value = str(concept_attr.get_value())
                            attr_type_label = concept_attr.get_type().get_label().name()
                            line_print += "$attr {0} isa {1}; ".format(attr_value, attr_type_label)
                        if print_relations:
                            # relation
                            concept_rel = dict_concept["rel"]
                            rel_iid = concept_rel.get_iid()
                            rel_type_label = concept_rel.get_type().get_label().name()
                            # role
                            concept_role = dict_concept["role"]
                            role_label = concept_role.get_label().name()
                            line_print += "$rel iid {0} ({1}:$x) isa {2}; ".format(rel_iid, role_label, rel_type_label)

                        print(line_print)
                        k+=1
                        if k==n:
                            break
    if return_client:
        return client
    else:
        client.close
        return None


def modify_each_concept(
    database,
    cluster=False,
    host="localhost",
    port="1729",
    username="admin",
    password="password", 
    tls_root_ca_path=None,
    addresses=[],
    session_type = "DATA",
    query_match = "match $x isa thing; get $x;",
    f_write = lambda write_transaction, iid : None,
    args=None,
    client=None,
    return_client=False,
    batch_size=50,
    parallelisation=2):
    '''@usage: iterate over all non-root concepts matching query, calling f_write.
            This makes it possibly to modify concepts individually, e.g. to assign ownership of a new attribute to 
            some concepts, or assign a unique identifier to each thing.
    @param database: the name of the database. string
    @param cluster: boolean, whether it is typedb cluster
    @param host, the host, string
    @param port, the port, string
    @param username for typedb cluster
    @param password for typedb cluster
    @param tls_root_ca_path full path to typedb cluster server/conf/encryption/rpc-root-ca.pem 
    @param addresses: list of string, each in the form "host:port" 
    @param session_type: one of "SCHEMA" or "DATA" 
    @param query_match: a data match query to retrieve all concepts to be modified. The concepts must be bound to $x.
                        If no matches a found, a message (but no error) is printed
    @param f_write: a function that takes a write transaction and an iid as first and second argument.
                Additional positional arguments can be passed through args.
                The function should NOT commit changes, as this is done in batches.
    @param args: a list of additional positional arguments to pass to f_write
    @param client: an active TypeDB client
    @param return_client: return TypeDB client, if not, client is closed
    @param batch_size: number of transactions before each write commit. Recommended <100
    @param parallellisation: passed to TypeDB client
    @return None
    '''
    if not session_type in ["SCHEMA", "DATA"]:
        raise ValueError('session_type must be one of "SCHEMA" or "DATA"')
    list_iid = []
    #list_out = []

    if cluster: 
        credential = TypeDBCredential(
            username=username, 
            password=password, 
            tls_root_ca_path=tls_root_ca_path)
        TypeDB.cluster_client(
            addresses = addresses,
            credential = credential,
            parallelisation=parallelisation)
    else:
        client = TypeDB.core_client(
            address=host+":"+port, 
            parallelisation=parallelisation)
    with client.session(database, SessionType.DATA if session_type == "DATA" else SessionType.SCHEMA) as session:
        with session.transaction(TransactionType.READ) as read_transaction:
            iterator_conceptMap = read_transaction.query().match(query_match)
            iterator_conceptMap = py_dev_utils.check_whether_iterator_empty(iterator_conceptMap)
            if not iterator_conceptMap is None:
                list_iid = [conceptMap.get("x").get_iid() for conceptMap in iterator_conceptMap]
            # read transaction closes

        if list_iid:
            k = 1
            write_transaction = session.transaction(TransactionType.WRITE)
            for iid in list_iid:
                f_write(write_transaction, iid, *args) if args else f_write(write_transaction,iid)
                if k%batch_size == 0:
                    print(f"write transactions: {k}")
                    write_transaction.commit()
                    write_transaction = session.transaction(TransactionType.WRITE)
                k+=1
            if write_transaction.is_open():
                write_transaction.commit()
        else:
            print("no concepts were returned by query_match")
        # session closes
    if not return_client:
        client.close
    else:
        return client


def delete_thingType_instances(
    database,
    thingType, 
    isa = "isa!",
    batch_size=1000,
    host="localhost",
    port="1729",
    parallelisation=4
    ):
    n_batches=1
    query_match = f"match $x {isa} {thingType}; limit {batch_size}; "
    with TypeDB.core_client(address=host+":"+port, parallelisation=parallelisation) as client:
        with client.session(database, SessionType.DATA) as session:
            while True:
                # read
                read_transaction = session.transaction(TransactionType.READ)
                iterator = read_transaction.query().match(query_match)
                iterator = py_dev_utils.check_whether_iterator_empty(iterator)
                if iterator:
                    n_batches += 1
                    if not n_batches % 10:
                        print("")
                        print(f"batch {n_batches}")
                    list_iid = [answer.get("x").get_iid() for answer in iterator] 
                    read_transaction.close()
                    # delete
                    list_query_delete = [f"match $x iid {iid}; delete $x isa {thingType}; " for iid in list_iid]
                    write_transaction = session.transaction(TransactionType.WRITE)
                    for query_delete in list_query_delete: 
                        write_transaction.query().delete(query_delete)
                    write_transaction.commit()
                else:
                    print("done deleting!")
                    break 
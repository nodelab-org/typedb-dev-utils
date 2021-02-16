from grakn.client import *

def del_db(database, host="localhost", port="1729"):
    '''@usage delete a grakn database
    @param database: the database to delete, string
    @param host, the host, string
    @param port, the port, string
    @return None
    '''
    with GraknClient.core(host+":"+port) as client:
        if client.databases().contains(database):
            client.databases().delete(database)
            print(database + " deleted")
        else:
            print(database + " not found")

        print("databases: {}".format(client.databases().all())) 


def init_db(
    database, 
    gql_schema=None, 
    parse_lines=False, 
    host="localhost", 
    port="1729"):
    '''
    @param database: the database to intialise, string
    @param gql_schema: path to schema, string
    @param parse_lines: whether to parse gql_schema line-by-line or as a whole. Bool, default False
    @param host, the host, string
    @param port, the port, string
    '''
    with GraknClient.core(host+":"+port) as client:
        client.databases().create(database)
        if not gql_schema is None:
            if parse_lines:
                f = open(gql_schema, "r")#
                with client.session(database, SessionType.SCHEMA) as session:
                    for line in f.readlines():
                        if all([token in line for token in ["define","sub",";"]]):
                            with session.transaction(TransactionType.WRITE) as write_transaction:
                                write_transaction.query().define(line)
                                write_transaction.commit()
            else:
                query_define = open(gql_schema, "r").read()
                with client.session(database, SessionType.SCHEMA) as session:
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        write_transaction.query().define(query_define)
                        write_transaction.commit()

        print("initiated " + database)              
        print("databases: {}".format(client.databases().all()))


def def_attr_type(
    database, 
    new_attr_label, 
    new_attr_value, 
    sup_label="attribute",
    is_key=False,
    thingtypes = ["entity", "relation", "attribute"], 
    host="localhost", 
    port="1729"):
    '''@usage: add a new attribute to all or subset of ThingTypes
    @param database: the name of the database. string 
    @param new_attr_label: the label of the new attribute. string
    @param new_attr_value: the value type of the new attribute, one of "long", "double", "string", "boolean" or "datetime". string
    @param sup: the supertype form which the new attributetype will inherit 
    @param is_key: is the attribute a key, bool
    @param thingtypes: list of one or more of ["entity", "relation", "attribute"]
    @param host: the host grakn is running on
    @param port: the port grakn is running on
    @return None 
    '''
    
    list_query_match = ["match $x sub {}; get $x;".format(thingtype) for thingtype in thingtypes]
    query_define_attr = "define {0} sub {1}, value {2};".format(new_attr_label, sup_label, new_attr_value)
    list_concept = [] 

    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                for query_match in list_query_match:
                    iterator_conceptMap = read_transaction.query().match(query_match)
                    for conceptMap in iterator_conceptMap:
                        if not conceptMap.get("x").get_label() in ["entity", "relation", "attribute"]:
                            list_concept.append(conceptMap.get("x"))
            
        #with client.session(database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                write_transaction.query().define(query_define_attr)
                write_transaction.commit()
        
        #with client.session(database, SessionType.SCHEMA) as session:
            for concept in list_concept:
                with session.transaction(TransactionType.READ) as read_transaction:
                    concept_sup_label = concept.as_remote(read_transaction).get_supertype().get_label()
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    query_define_owns = "define {0} sub {1}, owns {2}".format(concept.get_label(), concept_sup_label, new_attr_label)
                    # if is_key:
                    #     query_define_owns += " @key"
                    query_define_owns += ";"
                    print(query_define_owns)
                    write_transaction.query().define(query_define_owns)
                    write_transaction.commit()
                    


def def_rel_type(
    database, 
    new_rel_label, 
    dict_role_players,
    sup="relation",
    host="localhost", 
    port="1729"):
    '''@usage: add a new relationtype to the schema
    @param database: the name of the database. string 
    @param new_rel_label: the label of the new relation type. string
    @param dict_role_players: dict with role labels (string) as keys and arrays of role_player types (string) as values
           to make a role applicable to all descendents of one or more root types, provide one or more of the root type(s) ["entity", "relation", "attribute"]
    @param sup: the supertype form which the new relationtype will inherit 
    @param host: the host grakn is running on
    @param port: the port grakn is running on
    @return None 
    '''
    
    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.SCHEMA) as session:
            # check if any root types included
            for role_label in dict_role_players.keys():
                list_role_players = dict_role_players[role_label]
                for root_type in ["entity", "relation", "attribute"]:
                    if root_type in list_role_players:
                        with session.transaction(TransactionType.READ) as read_transaction:
                            query_sub = "match $x sub {}; get $x;".format(root_type)
                            iterator_conceptMap = read_transaction.query().match(query_sub)
                            for conceptMap in iterator_conceptMap:
                                dict_role_players[role_label].append(conceptMap.get("x").get_label())
                        # remove the root type from the role players
                        idx = dict_role_players[role_label].index(root_type)   
                        dict_role_players[role_label].pop(idx)
                        
            # prepare define relation query
            query_define_rel = "define {0} sub {1}, ".format(new_rel_label, sup)
            list_clause_relates = ["relates {}".format(role) for role in dict_role_players.keys()]
            query_define_rel += ", ".join(list_clause_relates) + ";"

            with session.transaction(TransactionType.WRITE) as write_transaction:
                # define relation type
                write_transaction.query().define(query_define_rel)
                write_transaction.commit()

            # add "plays" to existing types
            for role_label, list_role_player in dict_role_players.items():
                for role_player_label in list_role_player:
                    # get sup
                    with session.transaction(TransactionType.READ) as read_transaction:
                        role_player_concept = read_transaction.concepts().getThingType(role_player_label)
                        sup_label = role_player_concept.get_supertype().get_label()
                    query_define_plays = "define {0} sub {1}, plays {2}:{3};".format(role_player_label, sup_label, new_rel_label, role_label)
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        write_transaction.query().define(query_define_plays)
                        write_transaction.commit()


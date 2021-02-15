from grakn.client import *
import pkg_resources

def init_db(database, gql_schema=None, gql_data=None, host="localhost", port="1729"):
    '''
    @param database: the database to intialise, string
    @param gql_schema: path to schema, string
    @param gql_data: path to data, string
    @param host, the host, string
    @param port, the port, string
    '''

    
    with GraknClient.core(host+":"+port) as client:
        client.databases().create(database)
        with client.session(database, SessionType.SCHEMA) as session:
            if gql_schema is None: 
                gql_schema = pkg_resources.get_resource_filename(__name__, 'data/tenancy_schema.gql')
                # use packaged data
                #f = pkg_resources.resource_stream(__name__, 'data/tenancy_schema.gql').read()
            #else:
            f = open(gql_schema, "r")#
            for line in f.readlines():
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    if all([token in line for token in ["define","sub",";"]]):
                        write_transaction.query().define(line)
                        write_transaction.commit()
                
        with client.session(database, SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                if gql_data is None:
                    # use packaged data
                    gql_data=pkg_resources.get_resource_filename(__name__, 'data/tenancy_data.gql')
                #    f = pkg_resources.resource_stream(__name__, 'data/tenancy_data.gql').read()
                #else:
                f = open(gql_data, "r")#
                for line in f.readlines():
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        if all([token in line for token in ["insert",";"]]):
                            write_transaction.query().insert(line)
                            write_transaction.commit()

        print("databases: {}".format(client.databases().all()))


def del_db(database, host="localhost", port="1729"):
    '''@usage delete a grakn database
    @param database: the database to delete, string
    @param host, the host, string
    @param port, the port, string
    @return None
    '''
    with GraknClient.core(host+":"+port) as client:
        client.databases().delete(database)

        print("databases: {}".format(client.databases().all())) 



def def_attr_type(
    database, 
    new_attr_label, 
    new_attr_value, 
    sup="attribute",
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
    query_define_attr = "define {0} sub {1}, value {2};".format(new_attr_label, sup, new_attr_value)
    list_concept = [] 

    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                for query_match in list_query_match:
                    iterator_conceptMap = read_transaction.query().match(query_match)
                    for conceptMap in iterator_conceptMap:
                        list_concept.append(conceptMap.get("x"))

            with session.transaction(TransactionType.WRITE) as write_transaction:
                write_transaction.query().define(query_define_attr)
                write_transaction.commit()

            for concept in list_concept:
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    query_define_owns = "define $x type {}, owns {}".format(concept.label(), new_attr_label)
                    if is_key:
                        query_define_owns += " @key"
                    query_define_owns += ";"
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
                                dict_role_players[role_label].append(conceptMap.get("x").label())
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
                        sup_label = role_player_concept.get_supertype().label()
                    query_define_plays = "define {0} sub {1}, plays {2}:{3};".format(role_player_label, sup_label, new_rel_label, role_label)
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        write_transaction.query().define(query_define_plays)
                        write_transaction.commit()




def modify_things(
    database, 
    query_match = "match $x isa thing; get $x;",
    thing_function = lambda thing, write_transaction: None,
    args=None,
    host="localhost", 
    port="1729"):
    '''@usage: iterate over all non-root things matching query, calling thing_function
    @param database: the name of the database. string 
    @param thing_function: a function that takes a thing and a write transaction as first and second argument. 
                Additional positional arguments can be passed through args. 
                Optionally returns value
    @param args: a list of additional positional arguments to pass to thing_function after thing
    @param host: the host grakn is running on
    @param port: the port grakn is running on
    @return a list of values returned by thing_function (if None returned, list of None)
    '''
    # TODO: does the read transaction stay open?
    list_out = [] 
    with GraknClient.core(host+":"+port) as client:
        with client.session(database, SessionType.DATA) as session:
            with session.transaction(TransactionType.READ) as read_transaction:
                iterator_conceptMap = read_transaction.query().match(query_match)
                for conceptMap in iterator_conceptMap:
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        thing = conceptMap.get("x")
                        if len(args):
                            list_out.append(thing_function(thing, write_transaction, *args))
                        else:
                            list_out.append(thing_function(thing, write_transaction))
                        write_transaction.commit()
    return list_out 


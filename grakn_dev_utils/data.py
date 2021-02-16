from grakn.client import *

def insert_data(
    database, 
    gql_data=None, 
    parse_lines=False, 
    host="localhost", 
    port="1729"):
    '''
    @param database: the database to intialise, string
    @param gql_data: path to data, string
    @param parse_lines: whether to parse gql_data line-by-line or as a whole. bool, default False
    @param host, the host, string
    @param port, the port, string
    '''
    with GraknClient.core(host+":"+port) as client:          
        if parse_lines:
            f = open(gql_data, "r")
            with client.session(database, SessionType.DATA) as session:
                for line in f.readlines():
                    if all([token in line for token in ["insert",";"]]):
                        with session.transaction(TransactionType.WRITE) as write_transaction:
                            write_transaction.query().insert(line)
                            write_transaction.commit()
        else:
            query_insert = open(gql_data, "r").read()
            with client.session(database, SessionType.DATA) as session:
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    write_transaction.query().insert(query_insert)
                    write_transaction.commit()

        print("databases: {}".format(client.databases().all()))



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
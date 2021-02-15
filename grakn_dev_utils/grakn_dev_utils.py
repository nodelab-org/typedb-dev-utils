from grakn.client import *

def init(database, gql_schema, gql_data, host="localhost", port="1729"):
    with GraknClient.core(host+":"+port) as client:
        client.databases().create(database)
        with client.session(database, SessionType.SCHEMA) as session:
            f = open(gql_schema, "r")#
            k=1
            for line in f.readlines():
                with session.transaction(TransactionType.WRITE) as write_transaction:
                    # print("")
                    # print(line)
                    if all([token in line for token in ["define","sub",";"]]):
                        write_transaction.query().define(line)
                        write_transaction.commit()
                    k+=1
                
        with client.session(database, SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                f = open(gql_data, "r")#
                for line in f.readlines():
                    print("")
                    print(line)
                    with session.transaction(TransactionType.WRITE) as write_transaction:
                        if all([token in line for token in ["insert",";"]]):
                            write_transaction.query().insert(line)
                            write_transaction.commit()
                        k+=1

        print("databases: {}".format(client.databases().all()))


def del(database, host="localhost", port="1729"):
    with GraknClient.core(host+":"+port) as client:
        client.databases().delete(database)

        print("databases: {}".format(client.databases().all())) 
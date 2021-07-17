import pytest
import grakn_dev_utils as gradevils
from typedb.client import *

@pytest.fixture(scope="session")
def database_params():
    return {"database":"test",
        "gql_schema":"/Users/rkm916/Sync/data/grakn-database-tenancy/tenancy_schema.gql",
        "gql_data":"/Users/rkm916/Sync/data/grakn-database-tenancy/tenancy_data.gql",
        "host":"localhost",
        "port":"1729",
        "username":"admin",
        "password":"password", 
        "tls_root_ca_path":"/Users/rkm916/Sync/tools/typedb-cluster-all-mac-2.1.2_1/server/conf/encryption/rpc-root-ca.pem",
        "addresses":["127.0.0.1:11729","127.0.0.1:21729","127.0.0.1:31729"],
        "verbose":True}

@pytest.fixture(scope="function")
def db_client_core(database_params):
    gradevils.del_db(database_params["database"])
    gradevils.init_db(
        database_params["database"],
        gql_schema=database_params["gql_schema"], 
        parse_lines=False)
    yield TypeDB.core_client(database_params["host"]+":"+database_params["port"])
    # tear down
    gradevils.del_db(database_params["database"])

@pytest.fixture(scope="function")
def db_client_cluster(database_params):
    gradevils.del_db_cluster(
            database=database_params["database"],
            username=database_params["username"],
            password=database_params["password"],
            tls_root_ca_path=database_params["tls_root_ca_path"],
            addresses=database_params["addresses"]
        )
    gradevils.init_db_cluster(
        database_params["database"],
        username=database_params["username"],
        password=database_params["password"],
        tls_root_ca_path=database_params["tls_root_ca_path"],
        addresses=database_params["addresses"],
        gql_schema=database_params["gql_schema"], 
        parse_lines=False)
    
    credential = TypeDBCredential(
        username=database_params["username"], 
        password=database_params["password"], 
        tls_root_ca_path=database_params["tls_root_ca_path"])

    yield TypeDB.cluster_client(
            addresses = database_params["addresses"],
            credential = credential,
            parallelisation=2)

    # tear down
    gradevils.del_db_cluster(
        database_params["database"],
        username=database_params["username"],
        password=database_params["password"],
        tls_root_ca_path=database_params["tls_root_ca_path"],
        addresses=database_params["addresses"],
    )
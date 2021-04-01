import pytest
import grakn_dev_utils as gradevils
from grakn.client import *

@pytest.fixture(scope="session")
def database_params():
    return {"database":"test",
        "gql_schema":"/Users/rkm916/Sync/data/grakn-database-tenancy/tenancy_schema.gql",
        "gql_data":"/Users/rkm916/Sync/data/grakn-database-tenancy/tenancy_data.gql",
        "host":"localhost",
        "port":"1729",
        "verbose":True}

@pytest.fixture(scope="function")
def db_client(database_params):
    gradevils.del_db(database_params["database"])
    gradevils.init_db(
        database_params["database"],
        gql_schema=database_params["gql_schema"], 
        parse_lines=False)
    yield Grakn.core_client(database_params["host"]+":"+database_params["port"])
    # tear down
    gradevils.del_db(database_params["database"])
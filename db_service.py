from typing import Literal

from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder


# mongo class
class MongoDBConnection:
    _connection = None
    _current_environment = None
    _database = None
    _server = None
    _database_name = None

    @classmethod
    def get_connection(cls, db_environment: str):
        if db_environment in ["stage", "prod"]:
            if cls._server is not None:
                cls._server.stop()

            if db_environment == "stage":
                # Mongo Configuration for stage environment
                print("stage")
                MONGO_USER = ""
                MONGO_PASS = ""
                MONGO_URI = (
                    ""
                )
            elif db_environment == "prod":
                # Mongo Configuration for production environment
                print("prod")
                MONGO_USER = ""
                MONGO_PASS = ""
                MONGO_URI = (
                    ""
                )

            # VM IP/DNS
            EC2_URL = ""
            cls._server = SSHTunnelForwarder(
                (EC2_URL, 22),
                ssh_username="",
                ssh_pkey="",
                remote_bind_address=(MONGO_URI, 00000),
                local_bind_address=("127.0.0.1", 00000),
            )
            cls._server.start()

            # Connect to Database
            cls._connection = MongoClient(
                username=MONGO_USER,
                password=MONGO_PASS,
                tlsCAFile="",
                tlsAllowInvalidHostnames=True,
                directConnection=True,
                retryWrites=False,
            )
        else:
            if cls._current_environment == "local":
                print("local")
                cls._connection = MongoClient("")
            else:
                print("qa")
                cls._connection = MongoClient(
                    ""
                )
        return cls._connection

    @classmethod
    def get_db(cls, database_name: str, db_environment: str):
        if (
            cls._current_environment != db_environment
            or cls._database_name != database_name
        ):
            cls._database_name = database_name
            cls._current_environment = db_environment
            cls._connection = None
            connection = cls.get_connection(db_environment)
            cls._database = connection.get_database(database_name)
        return cls._database


def get_collection(
    database_name: str,
    col_name: str,
    client: Literal["local", "qa", "stage", "prod"] = "local",
):
    db = MongoDBConnection.get_db(database_name, client)
    return db.get_collection(col_name)

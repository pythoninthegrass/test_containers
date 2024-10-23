#!/usr/bin/env python

import os
import sqlalchemy
# from decouple import config
from pathlib import Path
from testcontainers.mssql import SqlServerContainer
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.container import DockerContainer

HOME = str(Path.home())

if not Path(f"{HOME}/.orbstack/run/docker.sock").exists():
    DOCKER_SOCKET = "unix:///var/run/docker.sock"
else:
    DOCKER_SOCKET = f"unix://{HOME}/.orbstack/run/docker.sock"

os.environ["DOCKER_HOST"] = DOCKER_SOCKET
os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = DOCKER_SOCKET

backup_file = Path(__file__).parent / "aw_lt_2017.bak"

if not backup_file.exists():
    print("Use bin/download.py to download the AdventureWorks backup file.")
    print("Example: python bin/download.py https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksLT2017.bak ./examples/ms_sql/aw_lt_2017.bak")
    exit(1)

adventureworks_bak_path = str(backup_file)

with SqlServerContainer(
    image="mcr.microsoft.com/mssql/server:2017-latest",
    username="SA",
    password="Strong@Password123",
    port=1433,
    environment={"ACCEPT_EULA": "Y"}
) as mssql:

    engine = sqlalchemy.create_engine(mssql.get_connection_url())

    with engine.begin() as connection:
        connection.execute(sqlalchemy.text(f"""
            RESTORE DATABASE AdventureWorks
            FROM DISK = '{adventureworks_bak_path}'
            WITH MOVE 'AdventureWorks_Data' TO '/var/opt/mssql/data/AdventureWorks.mdf',
                 MOVE 'AdventureWorks_Log' TO '/var/opt/mssql/data/AdventureWorks.ldf',
                 REPLACE
        """))

        result = connection.execute(sqlalchemy.text("""
            USE AdventureWorks;
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE';
        """))

        for row in result:
            print(row)

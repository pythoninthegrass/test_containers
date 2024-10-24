#!/usr/bin/env python

import io
import sqlalchemy
import tarfile
from dataclasses import dataclass, field
from decouple import config
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from testcontainers.mssql import SqlServerContainer
from time import sleep
from typing import Dict, List, Optional

# env vars
IMAGE = config("IMAGE", default="mcr.microsoft.com/mssql/server:2017-latest")
PORT = config("PORT", default=1433, cast=int)
BACKUP_FILE = config("BACKUP_FILE", default="aw_lt_2017.bak")
ACCEPT_EULA = config("ACCEPT_EULA", default="Y")
MSSQL_PID = config("MSSQL_PID", default="Developer")
MSSQL_USER = config("DB_USER", default="sa")
MSSQL_SA_PASSWORD = config("DB_PASS", default="Strong@Password123")
DATA_PATH = config("DATA_PATH", default="/var/opt/mssql/data")
BACKUP_PATH = config("BACKUP_PATH", default="/var/opt/mssql/backup")


@dataclass
class LogicalFiles:
    data: List[str] = field(default_factory=list)
    log: List[str] = field(default_factory=list)


@dataclass
class DatabaseInfo:
    database_name: str
    backup_type: str
    server_name: str
    backup_start_date: str
    backup_finish_date: str


@dataclass
class DatabaseRestorer:
    backup_file: Path
    data_path: str = DATA_PATH
    backup_path: str = BACKUP_PATH
    db_name: Optional[str] = None
    logical_files: LogicalFiles = field(default_factory=LogicalFiles)
    db_info: Optional[DatabaseInfo] = None

    def create_tar_with_file(self) -> io.BytesIO:
        """Create a tar archive containing the backup file."""
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tar.add(self.backup_file, arcname=self.backup_file.name)
        tar_stream.seek(0)
        return tar_stream

    def get_database_info(self, connection) -> DatabaseInfo:
        """Extract database information from backup file."""
        filequery = sqlalchemy.text(f"""
            RESTORE HEADERONLY
            FROM DISK = '{self.backup_path}/{self.backup_file.name}'
        """)
        header_info = connection.execute(filequery).mappings().first()

        self.db_info = DatabaseInfo(
            database_name=header_info['DatabaseName'],
            backup_type=header_info['BackupType'],
            server_name=header_info['ServerName'],
            backup_start_date=str(header_info['BackupStartDate']),
            backup_finish_date=str(header_info['BackupFinishDate'])
        )
        self.db_name = self.db_info.database_name

        return self.db_info

    def get_logical_files(self, connection) -> LogicalFiles:
        """Get logical file names from backup."""
        filequery = sqlalchemy.text(f"""
            RESTORE FILELISTONLY
            FROM DISK = '{self.backup_path}/{self.backup_file.name}'
        """)
        files = connection.execute(filequery).mappings().all()

        self.logical_files = LogicalFiles(
            data=[row['LogicalName'] for row in files if row['Type'] == 'D'],
            log=[row['LogicalName'] for row in files if row['Type'] == 'L']
        )

        return self.logical_files

    def generate_move_statements(self) -> str:
        """Generate MOVE statements for each logical file."""
        if not self.db_name or not self.logical_files:
            raise ValueError("Database info and logical files must be retrieved first")

        moves = []
        for data_file in self.logical_files.data:
            moves.append(f"MOVE '{data_file}' "
                        f"TO '{self.data_path}/{self.db_name}_{data_file}.mdf'")

        for log_file in self.logical_files.log:
            moves.append(f"MOVE '{log_file}' "
                        f"TO '{self.data_path}/{self.db_name}_{log_file}.ldf'")

        return ',\n        '.join(moves)

    def restore_database(self, connection) -> None:
        """Restore database using information from backup file."""
        if not self.db_name or not self.logical_files:
            raise ValueError("Database info and logical files must be retrieved first")

        move_statements = self.generate_move_statements()
        restore_query = sqlalchemy.text(f"""
            RESTORE DATABASE [{self.db_name}]
            FROM DISK = '{self.backup_path}/{self.backup_file.name}'
            WITH
                {move_statements},
                REPLACE,
                STATS = 10
        """)

        connection.execute(restore_query)

    def verify_restore(self, connection) -> list:
        """Verify database restore by listing all tables."""
        if not self.db_name:
            raise ValueError("Database info must be retrieved first")

        tables = connection.execute(sqlalchemy.text(f"""
            SELECT TABLE_NAME
            FROM [{self.db_name}].INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """))

        return [table[0] for table in tables]


def setup_mssql_container(backup_path: Path):
    """Set up MSSQL container and restore database from backup."""
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    mssql = SqlServerContainer(
        image=IMAGE,
        username=MSSQL_USER,
        password=MSSQL_SA_PASSWORD,
        port=PORT
    )

    print("Starting MSSQL container...")
    mssql.start()

    try:
        container = mssql.get_wrapped_container()
        container.exec_run(f"mkdir -p {BACKUP_PATH}")

        print("Copying backup file to container...")
        restorer = DatabaseRestorer(backup_path)
        tar_stream = restorer.create_tar_with_file()
        container.put_archive(BACKUP_PATH, tar_stream)

        sleep(5)  # Wait for SQL Server to be ready

        print("Connecting to database...")
        engine = sqlalchemy.create_engine(
            mssql.get_connection_url(),
            isolation_level='AUTOCOMMIT'
        )

        with engine.connect() as conn:
            print("Getting database information...")
            db_info = restorer.get_database_info(conn)
            print(f"Database to restore: {db_info.database_name}")
            print(f"Backup taken from: {db_info.server_name}")
            print(f"Backup started: {db_info.backup_start_date}")

            print("Checking backup file contents...")
            logical_files = restorer.get_logical_files(conn)
            print(f"\nData files found: {logical_files.data}")
            print(f"Log files found: {logical_files.log}")

            print("\nRestoring database...")
            restorer.restore_database(conn)

            print("Verifying database restore...")
            tables = restorer.verify_restore(conn)
            print("\nRestored tables:")
            for table in tables:
                print(f"- {table}")

        return mssql

    except Exception as e:
        print(f"Error: {str(e)}")
        mssql.stop()
        raise


def main():
    container = None
    try:
        backup_file = Path(__file__).parent / BACKUP_FILE
        container = setup_mssql_container(backup_file)
        print("\nDatabase restored successfully!")
        input("\nPress Enter to stop the container...")
    except Exception as e:
        print(f"Setup failed: {e}")
        raise
    finally:
        if container is not None:
            print("Stopping container...")
            container.stop()


if __name__ == "__main__":
    main()

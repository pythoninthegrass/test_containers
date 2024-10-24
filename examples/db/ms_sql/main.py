#!/usr/bin/env python

import io
import sqlalchemy
import tarfile
from decouple import config
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from testcontainers.mssql import SqlServerContainer
from time import sleep

# env vars
IMAGE = config("IMAGE", default="mcr.microsoft.com/mssql/server:2017-latest")
PORT = config("PORT", default=1433, cast=int)
BACKUP_FILE = config("BACKUP_FILE", default="aw_lt_2017.bak")
ACCEPT_EULA = config("ACCEPT_EULA", default="Y")
MSSQL_PID = config("MSSQL_PID", default="Developer")
MSSQL_USER = config("DB_USER", default="sa")
MSSQL_SA_PASSWORD = config("DB_PASS", default="Strong@Password123")

# backup file
backup_file = Path(__file__).parent / BACKUP_FILE


def create_tar_with_file(file_path: Path) -> io.BytesIO:
    """Create a tar archive containing the backup file."""
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        tar.add(file_path, arcname=file_path.name)
    tar_stream.seek(0)
    return tar_stream


def setup_mssql_container(backup_path: Path):
    """Set up MSSQL container and restore database from backup."""
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    # Create container with default settings
    mssql = SqlServerContainer(
        image=IMAGE,
        username=MSSQL_USER,
        password=MSSQL_SA_PASSWORD,
        port=PORT
    )

    print("Starting MSSQL container...")
    mssql.start()

    try:
        # Set up backup directory and copy file
        container = mssql.get_wrapped_container()
        container.exec_run("mkdir -p /var/opt/mssql/backup")

        # Create and copy tar archive
        print("Copying backup file to container...")
        tar_stream = create_tar_with_file(backup_path)
        container.put_archive("/var/opt/mssql/backup", tar_stream)

        # Wait a moment for SQL Server to be fully ready
        sleep(5)

        # Create database connection with specific isolation level
        print("Connecting to database...")
        engine = sqlalchemy.create_engine(
            mssql.get_connection_url(),
            isolation_level='AUTOCOMMIT'
        )

        with engine.connect() as conn:
            # First, get the logical file names from the backup
            print("Checking backup file contents...")
            filequery = sqlalchemy.text(f"""
                RESTORE FILELISTONLY
                FROM DISK = '/var/opt/mssql/backup/{backup_path.name}'
            """)
            files = conn.execute(filequery)

            # Get all columns and rows for debugging
            rows = [dict(zip(files.keys(), row)) for row in files]
            print("\nBackup file contents:")
            for row in rows:
                print(f"LogicalName: {row['LogicalName']}")
                print(f"PhysicalName: {row['PhysicalName']}")
                print(f"Type: {row['Type']}")
                print("---")

            # Map logical files
            logical_files = {
                'data': next(row['LogicalName'] for row in rows if row['Type'] == 'D'),
                'log': next(row['LogicalName'] for row in rows if row['Type'] == 'L')
            }

            print(f"\nMapped logical files: {logical_files}")

            # Restore database using the correct logical names
            print("\nRestoring database...")
            restore_query = sqlalchemy.text(f"""
                RESTORE DATABASE AdventureWorks
                FROM DISK = '/var/opt/mssql/backup/{backup_path.name}'
                WITH
                    MOVE '{logical_files['data']}'
                        TO '/var/opt/mssql/data/AdventureWorks.mdf',
                    MOVE '{logical_files['log']}'
                        TO '/var/opt/mssql/data/AdventureWorks.ldf',
                    REPLACE,
                    STATS = 10
            """)

            conn.execute(restore_query)

            # Verify restore
            print("Verifying database restore...")
            tables = conn.execute(sqlalchemy.text("""
                SELECT TABLE_NAME
                FROM AdventureWorks.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            """))

            print("\nRestored tables:")
            for table in tables:
                print(f"- {table[0]}")

        return mssql

    except Exception as e:
        print(f"Error: {str(e)}")
        mssql.stop()
        raise


def main():
    container = None

    try:
        container = setup_mssql_container(backup_file)
        print("\nDatabase restored successfully!")

        # Keep container running for testing if needed
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

# test_containers

Suite of examples on how to use [testcontainers](https://testcontainers.com/getting-started/) with [Python](https://testcontainers-python.readthedocs.io/en/latest/).

## Minimum Requirements

* macOS 14+
  * Linux _should_ work 🤞
* [Python 3.11+](https://www.python.org/downloads/)
* [Poetry](https://python-poetry.org/docs/)
* [Docker](https://docs.docker.com/get-docker/)

## Recommended Requirements

* [asdf](https://asdf-vm.com/#/)

## Setup

```bash
# Clone the repository
git clone https://github.com/pythoninthegrass/test_containers.git
cd test_containers

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install dependencies
## macos
./bin/build.py      # install freetds and compile flags for `pymssql`

## all
poetry install
```

## Quickstart

> [!NOTE]
> There is a [helper script](bin/download.py) in the `bin` directory to download the AdventureWorks2017.bak file. This file is used to restore the database in the examples.
> 
> Run via `./bin/download.py <URL> ./examples/db/ms_sql/aw_lt_2017.bak`
> 
> The URL is found at [AdventureWorks sample databases - SQL Server | Microsoft Learn](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms#download-backup-files).

```bash
# ms sql
cd examples/db/ms_sql/
λ ./main.py
using host tcp://127.0.0.1:57781
Starting MSSQL container...
...
Restoring database...
Verifying database restore...

Restored tables:
- ErrorLog
- BuildVersion
- Address
- Customer
- CustomerAddress
- Product
- ProductCategory
- ProductDescription
- ProductModel
- ProductModelProductDescription
- SalesOrderDetail
- SalesOrderHeader

Database restored successfully!

Press Enter to stop the container...
Stopping container...
```

## TODO

* Write a test that uses a testcontainer
* Add taskfile for task runners
* Add more examples
  * MongoDB
  * Postgres
  * Redis
  * Kafka
  * Elasticsearch

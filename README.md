# Enterprise Data Warehouse Modernization

## Overview

This project focuses on modernizing an Enterprise Data Warehouse (EDW) by migrating from legacy systems to a modern, scalable architecture. It involves developing robust ETL workflows using **Informatica** and **SSIS**, leveraging **MDX** to query and validate data in existing OLAP cubes, and using **KornShell** and **Python** scripts to automate file transfers and batch job scheduling.

## Objectives

* Migrate legacy EDW workflows to a modern platform.
* Ensure data integrity and consistency across systems.
* Improve ETL performance and maintainability.
* Automate operational processes to reduce manual effort.

## Tech Stack

* **ETL Tools**: Informatica, SSIS
* **Scripting**: KornShell, Python
* **Databases**: SQL Server, Oracle
* **Query Language**: MDX
* **Version Control**: Git
* **Scheduling**: Cron, Informatica Scheduler

## Features

* **Data Extraction**: Pulls data from multiple sources including ERP, CRM, and flat files.
* **Transformation**: Implements business logic and data quality checks.
* **Load**: Loads processed data into OLAP cubes and relational databases.
* **Automation**: Scripts for automated file transfers and batch processing.
* **Validation**: Uses MDX queries to validate cube data.

## Project Structure

```
├── README.md
├── etl_workflows/            # Informatica & SSIS workflows
├── scripts/                  # KornShell & Python automation scripts
├── mdx_queries/              # MDX validation scripts
├── data/                     # Sample datasets for testing
└── docs/                      # Design documents & migration plans
```

## Installation & Setup

1. Clone the repository:

```bash
git clone https://github.com/your-username/edw-modernization.git
```

2. Install required dependencies for Python scripts:

```bash
pip install -r requirements.txt
```

3. Configure database connection details in `config.json`.
4. Load sample datasets from `data/` folder.

## Usage

* Run ETL workflows via Informatica/SSIS.
* Execute automation scripts to handle file transfers:

```bash
./scripts/file_transfer.sh
```

* Validate OLAP cube data:

```bash
mdxcli -f mdx_queries/validate_sales.mdx
```

## Contributing

1. Fork the repo.
2. Create a new branch (`feature/new-feature`).
3. Commit changes.
4. Open a pull request.

## License

This project is licensed under the MIT License.

# Rocktop Kickstart - Astronomer Airflow Project

## Overview

This is an Astronomer Airflow project designed for data pipeline orchestration with focus on batch processing, data transfer between Snowflake and MSSQL, and continuous monitoring workflows. The project includes custom operators for data transfer operations and various DAGs for different processing scenarios.

## Project Contents

### DAGs
- **`batch_initiation_dag.py`**: Monitors batch completion status and triggers downstream processing
- **`daily_processing_dag.py`**: Handles daily data processing workflows
- **`fadata_dag.py`**: FA data processing pipeline
- **`Raw_Layer_Preparation.py`**: Prepares raw data layer for downstream processing
- **`ConnectivityTest.py`**: Tests connectivity between different data sources

### Custom Components
- **Custom Operators** (`include/custom_operators/`):
  - `snowflake_to_odbc_operator.py`: Transfers data from Snowflake to ODBC connections
- **Custom Sensors** (`include/custom_sensors/`):
  - `async_sql_sensor.py`: Asynchronous SQL sensor for monitoring metadata
- **Custom Triggers** (`include/custom_triggers/`):
  - `sql_trigger.py`: SQL-based trigger

## Prerequisites


## Installation & Setup

### 1. Install Astronomer CLI
- Follow the installation steps on the official [Astro CLI documentation](https://www.astronomer.io/docs/astro/cli/install-cli/) based on your OS

### 2. Clone and Initialize Project
```bash
git clone git@github.com:brunocmartins/rocktop-kickstart.git
cd rocktop-kickstart
```

### 3. Configure Airflow Connections
Before running the DAGs, configure the following Airflow connections in the Airflow UI or via `airflow_settings.yaml`:

#### Required Connections:
- **Snowflake connection**
  - Connection Type: `Snowflake`
  - Login: Your Snowflake username
  - Password: Your Snowflake password or private key passphrase
  - Schema: Your Snowflake schema
  - Extra:
    - Warehouse: Your Snowflake warehouse
    - Account: Your Snowflake account
    - Role: Your Snowflake role
    - Database: Your Snowflake database
    - Region: Your Snowflake region
    - Private key content / private key path: Your Snowflake private key content (base64 encoded) or path to your mapped private key file

- **Microsoft SQL Server connection**
  - Connection Type: `mssql`
  - Host: Your MSSQL server hostname/IP
  - Login: Your MSSQL username
  - Password: Your MSSQL password
  - Schema: Your MSSQL database name
  - Port: Your MSSQL database port

- **ODBC Connection** 
  - Connection Type: `odbc`
  - Host: Your database server hostname/IP in the format of `<database_host>,<database_port>`
  - Login: Your database username
  - Password: Your database password
  - Schema: Your database database name
  - Extra:
    - Driver: "ODBC Driver 18 for SQL Server"
    - TrustServerCertificate: "Yes"
    - ApplicationIntent: "ReadOnly"

## Running the Project

### Local Development

1. **Start Airflow locally**:
   ```bash
   astro dev start
   ```

2. **Access Airflow UI**:
   - URL: http://localhost:8080/
   - Username: `admin`
   - Password: `admin`

### Port Configuration
If ports 8080 or 5432 are already in use, you can:
- Stop existing containers using those ports, or
- Change ports by modifying the [Astronomer configuration](https://www.astronomer.io/docs/astro/cli/astro-config-set) (e.g., `astro config set webserver.port 8081`)

## Development Guidelines

### Adding New DAGs
1. Create new Python files in the `dags/` directory
2. Follow the existing naming conventions
3. Include proper documentation and tags
4. Test locally before committing

### Custom Operators
- Place new operators in `include/custom_operators/`
- Follow the existing operator patterns
- Include comprehensive docstrings
- Add proper error handling

### Testing
- Use the `tests/` directory for unit tests
- Test DAGs locally before deployment
- Verify connections and permissions

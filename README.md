# Snowflake Data Metric Function Manager

A configuration-driven solution for managing Snowflake Data Metric Functions (DMFs) at scale. This project provides a stored procedure and Streamlit UI to add, validate, and monitor DMFs across multiple tables from a centralized configuration table.

## Overview

Data Metric Functions (DMFs) are Snowflake's native data quality monitoring feature. This project simplifies DMF management by:

- Centralizing DMF configurations in a single table
- Automating DMF deployment across multiple tables
- Supporting both table-level DMFs (e.g., `ROW_COUNT`, `FRESHNESS`) and column-level DMFs (e.g., `NULL_COUNT`, `DUPLICATE_COUNT`)
- Providing a visual interface for managing DMF assignments

## Components

| File | Description |
|------|-------------|
| `scripts/config_table_ddl.sql` | DDL for the `DMF_CONFIG` configuration table |
| `scripts/sp_manage_dmf.sql` | Stored procedure to add/validate DMFs based on config |
| `scripts/sample_config_data.sql` | Example configuration entries |
| `scripts/dmf_manager_app.py` | Streamlit app for visual DMF management |

## Prerequisites

- Snowflake Enterprise Edition (DMFs require Enterprise)
- Appropriate privileges:
  - `CREATE PROCEDURE` on the target schema
  - `SELECT` on `INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES`
  - `ALTER TABLE` on target tables
  - `EXECUTE DATA METRIC FUNCTION` on system DMFs

## Installation

### 1. Create the configuration table

```sql
-- Run in your governance schema (e.g., DEMO.GOVERNANCE)
CREATE TABLE IF NOT EXISTS DMF_CONFIG (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    TABLE_NAME VARCHAR(255) NOT NULL,
    COLUMN_NAMES VARCHAR(4000),
    DMF_NAME VARCHAR(500) NOT NULL,
    DMF_SCHEDULE VARCHAR(500),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    NOTES VARCHAR(2000)
);
```

### 2. Create the stored procedure

```sql
-- Run the contents of scripts/sp_manage_dmf.sql
```

### 3. (Optional) Deploy the Streamlit app

```sql
-- Create a Streamlit app in Snowsight using scripts/dmf_manager_app.py
```

## Usage

### Configuration Table Schema

| Column | Description |
|--------|-------------|
| `DATABASE_NAME` | Target database containing the table |
| `SCHEMA_NAME` | Target schema containing the table |
| `TABLE_NAME` | Table to apply the DMF to |
| `COLUMN_NAMES` | Comma-separated columns for column-level DMFs. `NULL` for table-level DMFs |
| `DMF_NAME` | Fully qualified DMF name (e.g., `SNOWFLAKE.CORE.NULL_COUNT`) |
| `DMF_SCHEDULE` | Schedule: `N MINUTE`, `USING CRON <expr> <tz>`, or `TRIGGER_ON_CHANGES` |
| `IS_ACTIVE` | Enable/disable without deleting the config entry |
| `NOTES` | Documentation for this DMF assignment |

### Adding Configuration Entries

```sql
-- Table-level DMF (COLUMN_NAMES is NULL)
INSERT INTO DMF_CONFIG (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAMES, DMF_NAME, DMF_SCHEDULE, NOTES)
VALUES ('MY_DB', 'MY_SCHEMA', 'ORDERS', NULL, 'SNOWFLAKE.CORE.ROW_COUNT', 'TRIGGER_ON_CHANGES', 'Track order count');

-- Column-level DMF
INSERT INTO DMF_CONFIG (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAMES, DMF_NAME, DMF_SCHEDULE, NOTES)
VALUES ('MY_DB', 'MY_SCHEMA', 'CUSTOMERS', 'EMAIL', 'SNOWFLAKE.CORE.NULL_COUNT', '5 MINUTE', 'Monitor null emails');
```

### Running the Stored Procedure

```sql
-- Add all active DMFs from config
CALL SP_MANAGE_DMF('DEMO.GOVERNANCE.DMF_CONFIG', 'ADD', NULL);

-- Validate DMFs (test execution)
CALL SP_MANAGE_DMF('DEMO.GOVERNANCE.DMF_CONFIG', 'VALIDATE', NULL);

-- Add DMFs for a specific table only
CALL SP_MANAGE_DMF('DEMO.GOVERNANCE.DMF_CONFIG', 'ADD', 'TABLE_NAME = ''CUSTOMERS''');
```

### Procedure Parameters

| Parameter | Description |
|-----------|-------------|
| `CONFIG_TABLE` | Fully qualified name of the configuration table |
| `ACTION` | `ADD` to apply DMFs, `VALIDATE` to test DMF execution |
| `FILTER_CONDITION` | Optional WHERE clause filter (e.g., `TABLE_NAME = 'ORDERS'`) |

### Return Value

The procedure returns a JSON object with execution results:

```json
{
  "action": "ADD",
  "summary": {
    "total": 3,
    "success": 2,
    "errors": 1
  },
  "results": [
    {
      "table": "MY_DB.MY_SCHEMA.ORDERS",
      "dmf": "SNOWFLAKE.CORE.ROW_COUNT",
      "columns": "",
      "status": "SUCCESS",
      "sql": "ALTER TABLE MY_DB.MY_SCHEMA.ORDERS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()"
    }
  ]
}
```

## Supported DMFs

### Table-Level DMFs (COLUMN_NAMES = NULL)

- `SNOWFLAKE.CORE.ROW_COUNT` - Total row count
- `SNOWFLAKE.CORE.FRESHNESS` - Data freshness monitoring

### Column-Level DMFs

- `SNOWFLAKE.CORE.NULL_COUNT` - Count of NULL values
- `SNOWFLAKE.CORE.DUPLICATE_COUNT` - Count of duplicate values
- `SNOWFLAKE.CORE.UNIQUE_COUNT` - Count of unique values
- `SNOWFLAKE.CORE.NULL_PERCENT` - Percentage of NULL values
- Custom DMFs created with `CREATE DATA METRIC FUNCTION`

## Schedule Options

| Format | Example | Description |
|--------|---------|-------------|
| Minutes | `5 MINUTE` | Run every N minutes (5, 15, 30, 60, 720, 1440) |
| Cron | `USING CRON 0 8 * * * UTC` | Run at specific times |
| Trigger | `TRIGGER_ON_CHANGES` | Run when table data changes |

## Viewing DMF Results

Query the DMF results view to see metric outputs:

```sql
SELECT *
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TABLE_NAME = 'CUSTOMERS'
ORDER BY MEASUREMENT_TIME DESC;
```

## License

MIT License

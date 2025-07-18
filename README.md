# PostgreSQL Helper Functions

A Python package providing robust utilities for PostgreSQL database operations, query management, and data analysis workflows with enterprise-grade security.

## Features

- 🔄 **Automatic retry logic** with exponential backoff for unreliable connections
- 📊 **Seamless pandas integration** for data analysis
- 🔧 **SQL query templating** and parameter substitution
- 🔊 **Cross-platform notifications** when long queries complete
- ⚙️ **Environment-based configuration** for secure credential management
- 🛡️ **Comprehensive error handling** and logging
- 🔒 **SSL/TLS encryption** with CA certificate verification for AWS RDS and other cloud databases
- **Enhanced in v1.2.0:**
  - 🔐 **Full SSL support** with optional CA certificate verification
  - 🛡️ **Man-in-the-middle attack prevention** for production environments
  - ✅ **SSL connection testing** and diagnostics
- **Previous in v1.1.0:**
  - 🚀 **Advanced fallback methods** for SQLAlchemy/pandas compatibility issues
  - 🔍 **Enhanced debugging** and diagnostic capabilities

## What's New in Version 1.2.0 🔐

### SSL/TLS Security Features
- **Optional SSL configuration**: All SSL settings are optional with sensible defaults
- **CA certificate verification**: Support for AWS RDS CA certificates and custom CAs
- **Multiple SSL modes**: From basic encryption to full certificate verification
- **SSL connection testing**: Verify SSL status and view encryption details
- **Backward compatibility**: Existing code continues to work without changes

### Security Options
```python
# Basic SSL (encrypted connection)
engine = createPostgresqlEngine()  # Uses DB_SSL_MODE=require by default

# Maximum security with CA certificate verification
engine = createPostgresqlEngineWithCustomSSL(
    ssl_ca_cert="/path/to/rds-ca-2019-root.pem",
    ssl_mode="verify-full"
)

# Test SSL connection
ssl_info = test_ssl_connection(engine)
print(f"SSL Active: {ssl_info['ssl_active']}")
```

## Installation

### From Source (Development)
```bash
git clone https://github.com/yourusername/pg_helpers.git
cd pg_helpers
pip install -e .
```

### From GitHub (Specific Version)
```bash
pip install git+https://github.com/yourusername/pg_helpers.git@v1.2.0
```

### Dependencies
```bash
pip install pandas psycopg2-binary sqlalchemy python-dotenv
```

## Quick Start

### 1. Environment Setup

Create a `.env` file in your project root (NOT in this repository):

```env
# Required database credentials
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_database

# SSL Configuration (optional)
# SSL Mode options: disable, allow, prefer, require, verify-ca, verify-full
DB_SSL_MODE=require

# Optional: Path to CA certificate for maximum security
# DB_SSL_CA_CERT=/path/to/rds-ca-2019-root.pem

# For AWS RDS with maximum security:
# DB_SSL_MODE=verify-full
# DB_SSL_CA_CERT=./certs/rds-ca-2019-root.pem
```

### 2. Basic Usage

```python
from pg_helpers import createPostgresqlEngine, dataGrabber, queryCleaner, test_ssl_connection

# Create secure database connection (uses SSL by default)
engine = createPostgresqlEngine()

# Verify SSL connection
ssl_info = test_ssl_connection(engine)
print(f"SSL Active: {ssl_info.get('ssl_active', 'Unknown')}")

# Execute a simple query
data = dataGrabber("SELECT * FROM users LIMIT 10", engine)
print(data.head())

# Enable debugging for troubleshooting
data = dataGrabber("SELECT * FROM complex_view", engine, debug=True)

# Use query templates
query = queryCleaner(
    'queries/user_analysis.sql',
    list1=[100, 200, 300],
    varString1='$USER_IDS',
    startDate='2023-01-01',
    endDate='2023-12-31'
)
results = dataGrabber(query, engine)
```

### 3. Custom SSL Configuration

```python
from pg_helpers import createPostgresqlEngineWithCustomSSL

# Maximum security for production
engine = createPostgresqlEngineWithCustomSSL(
    ssl_ca_cert="/path/to/rds-ca-2019-root.pem",
    ssl_mode="verify-full"
)

# Basic SSL without certificate verification
engine = createPostgresqlEngineWithCustomSSL(
    ssl_mode="require"
)

# Disable SSL (not recommended for production)
engine = createPostgresqlEngineWithCustomSSL(
    ssl_mode="disable"
)
```

## Function Reference

### Database Operations

#### `createPostgresqlEngine()`
Creates a SQLAlchemy engine for PostgreSQL connections using environment variables with SSL support.

**Returns:** `sqlalchemy.Engine`

**Environment variables:**
- **Required:** `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`
- **Optional:** `DB_PORT` (default: 5432)
- **SSL Options:**
  - `DB_SSL_MODE` (default: "require")
  - `DB_SSL_CA_CERT` (path to CA certificate)
  - `DB_SSL_CERT` (client certificate)
  - `DB_SSL_KEY` (client key)

#### `createPostgresqlEngineWithCustomSSL(ssl_ca_cert=None, ssl_mode='require', ssl_cert=None, ssl_key=None)` **NEW in v1.2.0**
Creates a SQLAlchemy engine with custom SSL configuration, overriding environment variables.

**Parameters:**
- `ssl_ca_cert` (str, optional): Path to CA certificate file
- `ssl_mode` (str): SSL mode - "disable", "allow", "prefer", "require", "verify-ca", "verify-full"
- `ssl_cert` (str, optional): Path to client certificate file
- `ssl_key` (str, optional): Path to client key file

**Returns:** `sqlalchemy.Engine`

**SSL Modes Explained:**
- `disable`: No SSL connection
- `allow`: Try SSL, fallback to non-SSL
- `prefer`: Try SSL first, fallback to non-SSL
- `require`: **Default** - Require SSL, fail if unavailable
- `verify-ca`: Require SSL and verify server certificate
- `verify-full`: **Most secure** - Require SSL, verify certificate and hostname

#### `test_ssl_connection(engine=None)` **NEW in v1.2.0**
Tests SSL connection and displays SSL information.

**Parameters:**
- `engine` (optional): SQLAlchemy engine (creates one if not provided)

**Returns:** `dict` with SSL connection details including:
- `ssl_active`: Whether SSL is active
- `ssl_cipher`: Encryption cipher used
- `ssl_version`: SSL/TLS version
- `client_cert_present`: Whether client certificate is present

#### `dataGrabber(query, engine, limit='None', debug=False)` **ENHANCED in v1.1.0**
Executes SQL queries and returns pandas DataFrames with robust error handling.

**Parameters:**
- `query` (str): SQL query to execute
- `engine`: SQLAlchemy engine
- `limit` (str, optional): Row limit for results
- `debug` (bool, optional): Enable detailed logging and debugging output

**Returns:** `pandas.DataFrame`

**Features:**
- Multiple fallback methods for compatibility issues
- Automatic detection and handling of metadata interpretation errors
- Comprehensive error logging and debugging
- Execution timing display
- Cross-platform sound notifications
- Automatic error propagation for retry logic

#### `recursiveDataGrabber(query_dict, results_dict, n=1, max_attempts=50)`
Executes multiple queries with automatic retry and exponential backoff.

**Parameters:**
- `query_dict` (dict): Dictionary of {query_name: sql_string}
- `results_dict` (dict): Dictionary to store results
- `n` (int): Current attempt number
- `max_attempts` (int): Maximum retry attempts

**Returns:** `dict` with DataFrames or None for failed queries

**Use case:** Perfect for running multiple large queries overnight with unreliable connections.

#### `diagnose_connection_and_query(engine, query, limit=10)` **ENHANCED in v1.1.0**
Diagnostic function to help troubleshoot SQLAlchemy/pandas compatibility issues.

**Parameters:**
- `engine`: SQLAlchemy engine
- `query` (str): SQL query to diagnose
- `limit` (int): Number of rows to test with

**Returns:** `dict` with diagnostic information

### Query Utilities

#### `queryCleaner(file, list1='empty', varString1='empty', ...)`
Loads SQL files and substitutes parameters for dynamic queries.

**Parameters:**
- `file` (str): Path to SQL file
- `list1`: List to substitute (converts to comma-separated string)
- `varString1` (str): Placeholder string in SQL file
- `startDate/endDate`: Date range parameters

**Returns:** `str` - Processed SQL query

#### `listPrep(iList)`
Converts Python lists to SQL-compatible comma-separated strings.

**Parameters:**
- `iList`: List of integers, floats, strings, or single value

**Returns:** `str` - SQL-formatted string

## Advanced Usage

### SSL Security Configuration **NEW in v1.2.0**

#### For AWS RDS (Recommended)
```python
from pg_helpers import createPostgresqlEngineWithCustomSSL, test_ssl_connection

# Download RDS CA certificate first
# wget https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem

# Maximum security configuration
engine = createPostgresqlEngineWithCustomSSL(
    ssl_ca_cert="./certs/rds-ca-2019-root.pem",
    ssl_mode="verify-full"
)

# Test the secure connection
ssl_info = test_ssl_connection(engine)
print(f"SSL Cipher: {ssl_info.get('ssl_cipher')}")
print(f"SSL Version: {ssl_info.get('ssl_version')}")
```

#### Environment-based SSL Configuration
```python
# Set in .env file:
# DB_SSL_MODE=verify-full
# DB_SSL_CA_CERT=./certs/rds-ca-2019-root.pem

from pg_helpers import createPostgresqlEngine, test_ssl_connection

# Uses environment variables
engine = createPostgresqlEngine()

# Verify security
ssl_info = test_ssl_connection(engine)
if ssl_info.get('ssl_active'):
    print("✅ Secure SSL connection established")
else:
    print("⚠️ SSL not active - check configuration")
```

#### SSL Mode Comparison
```python
# Different security levels for different environments

# Development (basic encryption)
dev_engine = createPostgresqlEngineWithCustomSSL(ssl_mode="require")

# Staging (certificate verification)
staging_engine = createPostgresqlEngineWithCustomSSL(
    ssl_mode="verify-ca",
    ssl_ca_cert="./certs/staging-ca.pem"
)

# Production (maximum security)
prod_engine = createPostgresqlEngineWithCustomSSL(
    ssl_mode="verify-full",
    ssl_ca_cert="./certs/rds-ca-2019-root.pem"
)
```

### Debugging and Troubleshooting
```python
from pg_helpers import createPostgresqlEngine, dataGrabber, diagnose_connection_and_query

engine = createPostgresqlEngine()

# Enable detailed debugging for problematic queries
data = dataGrabber("""
    SELECT * FROM complex_view 
    WHERE date_column > '2023-01-01'
""", engine, debug=True)

# Diagnose specific issues
diagnostics = diagnose_connection_and_query(engine, "SELECT * FROM problematic_table")
print(f"Engine info: {diagnostics['engine_info']}")
print(f"Test results: {diagnostics['test_results']}")
print(f"Recommendations: {diagnostics['recommendations']}")
```

### Batch Query Processing
```python
from pg_helpers import recursiveDataGrabber, queryCleaner

# Prepare multiple queries
queries = {
    'user_stats': queryCleaner('sql/user_stats.sql', startDate='2023-01-01'),
    'sales_data': queryCleaner('sql/sales.sql', list1=[1,2,3], varString1='$REGIONS'),
    'inventory': 'SELECT * FROM inventory WHERE status = "active"'
}

# Execute with automatic retry
results = {}
final_results = recursiveDataGrabber(queries, results)

# Access individual results
user_df = final_results['user_stats']
sales_df = final_results['sales_data']
```

## Security Best Practices

### SSL/TLS Configuration
- **Always use SSL in production** - Set `DB_SSL_MODE=require` minimum
- **Use CA certificate verification** for cloud databases (AWS RDS, GCP Cloud SQL)
- **Download official CA certificates** from your cloud provider
- **Use `verify-full` mode** for maximum security in production
- **Test SSL connections** regularly with `test_ssl_connection()`

### AWS RDS Security Setup
```bash
# Download AWS RDS CA certificate
mkdir -p ./certs
wget -O ./certs/rds-ca-2019-root.pem \
  https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem

# Set environment variables
echo "DB_SSL_MODE=verify-full" >> .env
echo "DB_SSL_CA_CERT=./certs/rds-ca-2019-root.pem" >> .env
```

### General Security
- **Never commit `.env` files** - add to `.gitignore`
- **Use environment variables** for all credentials
- **Rotate database passwords** regularly
- **Use database connection pooling** for production
- **Monitor SSL certificate expiration** dates

## File Structure for Your Projects

```
your_project/
├── .env                 # Database credentials (DO NOT COMMIT)
├── .gitignore          # Include .env and certs/
├── main.py             # Your analysis code
├── certs/              # SSL certificates (DO NOT COMMIT private keys)
│   ├── rds-ca-2019-root.pem
│   └── custom-ca.pem
├── queries/            # SQL template files
│   ├── user_analysis.sql
│   ├── sales_report.sql
│   └── inventory.sql
└── data/              # Output data files
    └── results.pkl
```

## Troubleshooting

### Common Issues

**SSL certificate verification failed**
- Ensure CA certificate file exists and is readable
- Download the latest CA certificate from your provider
- Check that `ssl_mode` is set correctly
- Use `test_ssl_connection()` to diagnose SSL issues

**"SSL connection required"**
- Check that your database server supports SSL
- Verify `DB_SSL_MODE` is set correctly
- For development, you can temporarily use `ssl_mode="disable"`

**"Missing required environment variables"**
- Ensure `.env` file exists in your project root
- Check that all required variables are set: `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`

**Connection timeouts**
- The `recursiveDataGrabber` will automatically retry with exponential backoff
- Check network connectivity and database server status
- Consider increasing `max_attempts` for very unreliable connections

**SQLAlchemy/pandas compatibility errors** **IMPROVED in v1.1.0**
- The package now automatically handles most compatibility issues
- Enable `debug=True` to see which fallback method succeeds
- Use `diagnose_connection_and_query()` for detailed troubleshooting

**"immutabledict" or metadata interpretation errors** **FIXED in v1.1.0**
- These errors are now automatically detected and handled
- The package will try alternative pandas parameters and manual DataFrame construction
- No action needed from users - fallback methods handle this automatically

**Sound notifications not working**
- This is normal and won't affect functionality
- Ensure system sound is enabled
- On Linux, you may need to install additional audio packages

## Changelog

### Version 1.2.0 (Current)
- 🔒 **SSL/TLS support**: Full SSL encryption with optional CA certificate verification
- 🛡️ **Security enhancements**: Man-in-the-middle attack prevention for production environments  
- ✅ **SSL testing**: New `test_ssl_connection()` function for diagnostics
- 🔧 **Custom SSL configuration**: Programmatic SSL parameter override
- 📋 **Environment SSL config**: Optional SSL settings via environment variables
- 🔄 **Backward compatibility**: Existing code continues to work without changes

### Version 1.1.0
- ✨ **Enhanced error handling**: Multiple fallback methods for pandas/SQLAlchemy compatibility
- 🔍 **Improved debugging**: Comprehensive logging and diagnostic capabilities
- 🛠️ **Better reliability**: Automatic detection and handling of metadata interpretation errors
- 📊 **Manual DataFrame construction**: Fallback method for complex data type issues
- 🔧 **Alternative parameter testing**: Tries different pandas configurations automatically

### Version 1.0.0
- Initial release with core functionality
- Basic retry logic and PostgreSQL integration
- Query templating and notification system

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Add tests for new functionality
4. Run tests: `python -m pytest tests/`
5. Submit a pull request

## License

MIT License - feel free to use in your projects!

## Author

Chris Leonard

---

*This package was designed for data analysts and engineers who work with PostgreSQL databases and need reliable, automated query execution with enterprise-grade security.*

# PostgreSQL Helper Functions

A Python package providing robust utilities for PostgreSQL database operations, query management, and data analysis workflows.

## Features

- 🔄 **Automatic retry logic** with exponential backoff for unreliable connections
- 📊 **Seamless pandas integration** for data analysis
- 🔧 **SQL query templating** and parameter substitution
- 🔊 **Cross-platform notifications** when long queries complete
- ⚙️ **Environment-based configuration** for secure credential management
- 🛡️ **Comprehensive error handling** and logging

## Installation

### From Source (Development)
```bash
git clone https://github.com/yourusername/pg_helpers.git
cd pg_helpers
pip install -e .
```

### Dependencies
```bash
pip install pandas psycopg2-binary sqlalchemy python-dotenv
```

## Quick Start

### 1. Environment Setup

Create a `.env` file in your project root (NOT in this repository):

```env
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_database
```

### 2. Basic Usage

```python
from pg_helpers import createPostgresqlEngine, dataGrabber, queryCleaner

# Create database connection
engine = createPostgresqlEngine()

# Execute a simple query
data = dataGrabber("SELECT * FROM users LIMIT 10", engine)
print(data.head())

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

## Function Reference

### Database Operations

#### `createPostgresqlEngine()`
Creates a SQLAlchemy engine for PostgreSQL connections using environment variables.

**Returns:** `sqlalchemy.Engine`

**Environment variables required:**
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password  
- `DB_HOST`: Database host
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name

#### `dataGrabber(query, engine, limit='None')`
Executes SQL queries and returns pandas DataFrames with timing and notifications.

**Parameters:**
- `query` (str): SQL query to execute
- `engine`: SQLAlchemy engine
- `limit` (str, optional): Row limit for results

**Returns:** `pandas.DataFrame`

**Features:**
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

### Query Utilities

#### `queryCleaner(file, list1='empty', varString1='empty', ...)`
Loads SQL files and substitutes parameters for dynamic queries.

**Parameters:**
- `file` (str): Path to SQL file
- `list1`: List to substitute (converts to comma-separated string)
- `varString1` (str): Placeholder string in SQL file
- `startDate/endDate`: Date range parameters

**Returns:** `str` - Processed SQL query

**Example SQL template:**
```sql
-- queries/user_analysis.sql
SELECT user_id, name, created_date 
FROM users 
WHERE user_id IN ($USER_IDS)
  AND created_date BETWEEN $START_DATE AND $END_DATE
ORDER BY created_date DESC;
```

#### `listPrep(iList)`
Converts Python lists to SQL-compatible comma-separated strings.

**Parameters:**
- `iList`: List of integers, floats, strings, or single value

**Returns:** `str` - SQL-formatted string

**Examples:**
```python
listPrep([1, 2, 3])        # Returns: "1,2,3"
listPrep(['a', 'b', 'c'])  # Returns: "'a','b','c'"
listPrep(42)               # Returns: "42"
```

## Advanced Usage

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

### Error Handling
```python
try:
    engine = createPostgresqlEngine()
    data = dataGrabber("SELECT * FROM large_table", engine, limit='1000')
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"Database error: {e}")
```

## File Structure for Your Projects

```
your_project/
├── .env                 # Database credentials (DO NOT COMMIT)
├── main.py             # Your analysis code
├── queries/            # SQL template files
│   ├── user_analysis.sql
│   ├── sales_report.sql
│   └── inventory.sql
└── data/              # Output data files
    └── results.pkl
```

## Best Practices

### Security
- **Never commit `.env` files** - add to `.gitignore`
- Use environment variables for all credentials
- Consider using database connection pooling for production

### Performance  
- Use `limit` parameter for testing large queries
- Leverage `recursiveDataGrabber` for batch processing
- Save intermediate results with pickle for long-running analyses

### Query Organization
- Store complex SQL in separate `.sql` files
- Use descriptive placeholder names (`$USER_IDS`, `$START_DATE`)
- Comment your SQL templates for team collaboration

## Troubleshooting

### Common Issues

**"Missing required environment variables"**
- Ensure `.env` file exists in your project root
- Check that all required variables are set: `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_NAME`

**Connection timeouts**
- The `recursiveDataGrabber` will automatically retry with exponential backoff
- Check network connectivity and database server status
- Consider increasing `max_attempts` for very unreliable connections

**Sound notifications not working**
- This is normal and won't affect functionality
- Ensure system sound is enabled
- On Linux, you may need to install additional audio packages

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

*This package was designed for data analysts and engineers who work with PostgreSQL databases and need reliable, automated query execution with proper error handling.*
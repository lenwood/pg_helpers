# Changelog

### Version 1.3.4 — 2026-07-22 (Current)
- **Package surface fixed**: `check_ssl_connection`, `createPostgresqlEngineWithCustomSSL`, and `diagnose_connection_and_query` are now exported from the package top level, and `__version__` reflects the real release
- **Safer `listPrep`**: passing an empty list now raises a clear `ValueError` instead of an opaque `IndexError`
- **Connection reliability**: engines are created with `pool_pre_ping=True` so dead pooled connections are recycled automatically during long/overnight runs
- **Performance**: the manual-construction query fallback builds the DataFrame directly from rows instead of an intermediate list of dicts
- **Type hints**: all public and private functions now have type hints
- **New `.env.example`**: template for required and optional environment variables
- **Documentation**: emojis removed from the documentation prose

### Version 1.3.3 — 2025-10-10
- **Updated installation instructions**: Recommend using 'pip install pg_helpers'

### Version 1.3.2 — 2025-10-10
- **Flexible credential file location**: Support for `CREDENTIALS_DIR` and `CREDENTIALS_FILE` environment variables
- **Enhanced configuration**: Specify custom .env file paths for different projects and environments
- **Centralized credentials**: Share credential directories across multiple projects
- **Environment-specific files**: Easy support for .env.production, .env.staging, .env.dev
- **Backwards compatible**: Defaults to `.env` in current directory when no variables set
- **Documentation**: New credential management strategies and best practices guide

### Version 1.3.1 — 2025-07-25
- **Comprehensive test suite**: 40+ test cases with >90% code coverage
- **Cross-platform validation**: Tests confirm functionality on Windows, macOS, and Linux
- **API improvements**: `test_ssl_connection()` renamed to `check_ssl_connection()` for clarity
- **CI/CD ready**: GitHub Actions workflow for automated testing across environments
- **Enhanced documentation**: Detailed testing guide and troubleshooting section
- **Quality assurance**: All functions tested including error conditions and edge cases

### Version 1.2.0 — 2025-07-18
- **SSL/TLS support**: Full SSL encryption with optional CA certificate verification
- **Security enhancements**: Man-in-the-middle attack prevention for production environments
- **SSL testing**: New SSL connection diagnostics
- **Custom SSL configuration**: Programmatic SSL parameter override
- **Environment SSL config**: Optional SSL settings via environment variables
- **Backward compatibility**: Existing code continues to work without changes

### Version 1.1.0 — 2025-06-27
- **Enhanced error handling**: Multiple fallback methods for pandas/SQLAlchemy compatibility
- **Improved debugging**: Comprehensive logging and diagnostic capabilities
- **Better reliability**: Automatic detection and handling of metadata interpretation errors
- **Manual DataFrame construction**: Fallback method for complex data type issues
- **Alternative parameter testing**: Tries different pandas configurations automatically

### Version 1.0.0 — 2025-05-29
- Initial release with core functionality
- Basic retry logic and PostgreSQL integration
- Query templating and notification system

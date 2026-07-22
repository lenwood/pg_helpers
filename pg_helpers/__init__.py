### pg_helpers/__init__.py
"""
PostgreSQL Helper Functions
A collection of utilities for PostgreSQL database operations and data analysis.
"""

from .database import (
    createPostgresqlEngine,
    createPostgresqlEngineWithCustomSSL,
    dataGrabber,
    recursiveDataGrabber,
    check_ssl_connection,
    diagnose_connection_and_query
)
from .query_utils import (
    listPrep,
    queryCleaner
)
from .notifications import play_notification_sound

__version__ = "1.3.4"
__author__ = "Chris Leonard"

# Make main functions available at package level
__all__ = [
    'createPostgresqlEngine',
    'createPostgresqlEngineWithCustomSSL',
    'dataGrabber',
    'recursiveDataGrabber',
    'check_ssl_connection',
    'diagnose_connection_and_query',
    'listPrep',
    'queryCleaner',
    'play_notification_sound'
]

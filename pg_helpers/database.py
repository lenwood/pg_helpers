### pg_helpers/database.py
"""Database connection and query execution utilities"""
import math
import os
import pandas as pd
import pickle
import time
from sqlalchemy import create_engine

from .config import validate_db_config
from .notifications import play_notification_sound

def createPostgresqlEngine():
    """
    Create SQLAlchemy engine for PostgreSQL connection
    
    Returns:
        sqlalchemy.Engine: Database engine
        
    Raises:
        ValueError: If required environment variables are missing
        Exception: If engine creation fails
    """
    try:
        config = validate_db_config()
        engine = create_engine(
            f"postgresql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}?sslmode=require"
        )
        return engine
    except Exception as e:
        print(f"Error creating PostgreSQL engine: {e}")
        raise

def dataGrabber(query, engine, limit='None'):
    """
    Execute query using SQLAlchemy engine and return pandas DataFrame
    
    Args:
        query (str): SQL query to execute
        engine: SQLAlchemy engine
        limit (str): Optional limit for query results
        
    Returns:
        pandas.DataFrame: Query results
    """
    # Handle limit for PostgreSQL
    if limit != 'None':
        limitString = f'LIMIT {limit}'
        query = query.replace('FOR READ ONLY', limitString + ' FOR READ ONLY')

    start = time.time()
    try:
        # Use pandas read_sql with SQLAlchemy engine
        data = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error in dataGrabber: {e}")
        raise  # Re-raise to be caught by recursiveDataGrabber
    
    end = time.time()
    elapsed = end - start
    
    m, s = divmod(elapsed, 60)
    h, m = divmod(m, 60)
    
    totTime = 'Elapsed Time: '
    eTime = "%d:%02d:%02d" % (h, m, s)
    totTime += eTime
    print(totTime)
    
    # Play notification sound
    play_notification_sound()

    return data

def recursiveDataGrabber(query_dict, results_dict, n=1, max_attempts=50):
    """
    Recursively execute queries with exponential backoff retry logic
    
    Args:
        query_dict (dict): Dictionary of query names and SQL strings
        results_dict (dict): Dictionary to store results
        n (int): Current attempt number
        max_attempts (int): Maximum number of retry attempts
        
    Returns:
        dict: Results dictionary with DataFrames or None for failed queries
    """
    def ordinal(i):
        return str(i) + {1:"st", 2:"nd", 3:"rd"}.get(i%10*(i%100 not in [11,12,13]), "th")
    
    if n <= max_attempts:
        if n > 1:
            print(f"{ordinal(n)} attempt")
            # Exponential backoff: Wait before retrying
            wait_time = min(2**(n-1), 600)  # Max wait time of 10 minutes
            print(f"Waiting for {wait_time:.2f} seconds before retry...")
            time.sleep(wait_time)
        
        engine = None
        try:
            # Create PostgreSQL engine
            engine = createPostgresqlEngine()
            
            # Test the connection
            with engine.connect() as conn:
                print("PostgreSQL database connection successful!")
            
            for k, v in query_dict.items():
                try:
                    results_dict[k] = dataGrabber(v, engine)
                    print(f"Query '{k}' completed successfully")
                except Exception as e:
                    print(f"Query '{k}' failed: {e}")
                    results_dict[k] = None
            
            # Save results
            dumpname = f'../Data/postgresql_results_attempt_{n}.pkl'
            os.makedirs(os.path.dirname(dumpname), exist_ok=True)
            
            with open(dumpname, 'wb') as f:
                pickle.dump(results_dict, f)
            
        except Exception as e:
            print(f"Database connection error at attempt {n}: {e}")
            for k in query_dict.keys():
                if k not in results_dict or not isinstance(results_dict[k], pd.DataFrame):
                    results_dict[k] = None
        
        finally:
            if engine:
                try:
                    engine.dispose()
                    print('PostgreSQL database connection closed.')
                except Exception as e:
                    print(f"Error closing engine: {e}")
        
        # Determine which queries need retry
        redo_dict = {k: v for k, v in query_dict.items() 
                    if not isinstance(results_dict.get(k), pd.DataFrame) or results_dict.get(k) is None}
        
        if redo_dict:
            failed_query_names = ", ".join(redo_dict.keys())
            print(f"{len(redo_dict)} {'query' if len(redo_dict) == 1 else 'queries'} remain: {failed_query_names}")
            return recursiveDataGrabber(redo_dict, results_dict, n+1, max_attempts)
        else:
            print("All queries completed successfully!")
            return results_dict
    
    else:
        print(f"Maximum retry attempts ({max_attempts}) reached.")
        failed_queries = [k for k, v in query_dict.items() 
                         if not isinstance(results_dict.get(k), pd.DataFrame) or results_dict.get(k) is None]
        if failed_queries:
            print(f"Failed queries: {', '.join(failed_queries)}")
        return results_dict

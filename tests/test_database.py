# tests/test_database.py
import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import os
import tempfile

# Import your package modules
from pg_helpers.query_utils import listPrep, queryCleaner
from pg_helpers.config import get_db_config, validate_db_config
from pg_helpers.database import createPostgresqlEngine, dataGrabber


class TestQueryUtils(unittest.TestCase):
    """Test query utility functions"""
    
    def test_listPrep_integers(self):
        """Test listPrep with integer list"""
        result = listPrep([1, 2, 3, 4])
        self.assertEqual(result, "1,2,3,4")
    
    def test_listPrep_floats(self):
        """Test listPrep with float list"""
        result = listPrep([1.1, 2.2, 3.3])
        self.assertEqual(result, "1.1,2.2,3.3")
    
    def test_listPrep_strings(self):
        """Test listPrep with string list"""
        result = listPrep(['apple', 'banana', 'cherry'])
        self.assertEqual(result, "'apple','banana','cherry'")
    
    def test_listPrep_single_value(self):
        """Test listPrep with single value"""
        result = listPrep("single_value")
        self.assertEqual(result, "single_value")
    
    def test_queryCleaner_basic(self):
        """Test basic query cleaning functionality"""
        # Create a temporary SQL file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as f:
            f.write("SELECT * FROM table WHERE id IN ($IDS) AND date BETWEEN $START_DATE AND $END_DATE")
            temp_file = f.name
        
        try:
            # Test query cleaning
            result = queryCleaner(
                file=temp_file,
                list1=[1, 2, 3],
                varString1='$IDS',
                startDate='2023-01-01',
                endDate='2023-12-31'
            )
            
            expected = "SELECT * FROM table WHERE id IN (1,2,3) AND date BETWEEN '2023-01-01' AND '2023-12-31'"
            self.assertEqual(result, expected)
        
        finally:
            # Clean up temporary file
            os.unlink(temp_file)
    
    def test_queryCleaner_no_substitutions(self):
        """Test query cleaner with no substitutions"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as f:
            original_query = "SELECT * FROM table"
            f.write(original_query)
            temp_file = f.name
        
        try:
            result = queryCleaner(file=temp_file)
            self.assertEqual(result, original_query)
        finally:
            os.unlink(temp_file)


class TestConfig(unittest.TestCase):
    """Test configuration functions"""
    
    @patch.dict(os.environ, {
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'testdb'
    })
    def test_get_db_config_complete(self):
        """Test getting complete database configuration"""
        config = get_db_config()
        expected = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': '5432',
            'database': 'testdb'
        }
        self.assertEqual(config, expected)
    
    @patch.dict(os.environ, {
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'DB_HOST': 'localhost',
        'DB_NAME': 'testdb'
        # DB_PORT is missing, should default to 5432
    })
    def test_get_db_config_default_port(self):
        """Test default port assignment"""
        config = get_db_config()
        self.assertEqual(config['port'], '5432')
    
    @patch.dict(os.environ, {
        'DB_USER': 'testuser',
        'DB_HOST': 'localhost',
        'DB_NAME': 'testdb'
        # DB_PASSWORD is missing
    }, clear=True)
    def test_validate_db_config_missing_password(self):
        """Test validation with missing required config"""
        with self.assertRaises(ValueError) as context:
            validate_db_config()
        
        self.assertIn("Missing required environment variables", str(context.exception))
        self.assertIn("password", str(context.exception))


class TestDatabase(unittest.TestCase):
    """Test database functions"""
    
    @patch('pg_helpers.database.validate_db_config')
    @patch('pg_helpers.database.create_engine')
    def test_createPostgresqlEngine_success(self, mock_create_engine, mock_validate):
        """Test successful engine creation"""
        # Mock the config validation
        mock_validate.return_value = {
            'user': 'testuser',
            'password': 'testpass',
            'host': 'localhost',
            'port': '5432',
            'database': 'testdb'
        }
        
        # Mock the engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Test the function
        result = createPostgresqlEngine()
        
        # Verify the engine was created with correct connection string
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn('postgresql://testuser:testpass@localhost:5432/testdb', call_args)
        self.assertEqual(result, mock_engine)
    
    @patch('pg_helpers.database.validate_db_config')
    def test_createPostgresqlEngine_config_error(self, mock_validate):
        """Test engine creation with config error"""
        # Mock config validation to raise an error
        mock_validate.side_effect = ValueError("Missing required environment variables: ['password']")
        
        # Test that the error is propagated
        with self.assertRaises(ValueError):
            createPostgresqlEngine()
    
    @patch('pg_helpers.database.play_notification_sound')
    @patch('pandas.read_sql')
    @patch('time.time')
    def test_dataGrabber_success(self, mock_time, mock_read_sql, mock_sound):
        """Test successful data grabbing"""
        # Mock time for elapsed time calculation
        mock_time.side_effect = [0, 5]  # 5 second execution
        
        # Mock pandas read_sql
        mock_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        mock_read_sql.return_value = mock_df
        
        # Mock engine
        mock_engine = MagicMock()
        
        # Test the function
        with patch('builtins.print') as mock_print:
            result = dataGrabber("SELECT * FROM test", mock_engine)
        
        # Verify results
        pd.testing.assert_frame_equal(result, mock_df)
        mock_read_sql.assert_called_once_with("SELECT * FROM test", mock_engine)
        mock_sound.assert_called_once()
        
        # Check that elapsed time was printed
        mock_print.assert_called_with('Elapsed Time: 0:00:05')
    
    @patch('pandas.read_sql')
    def test_dataGrabber_query_error(self, mock_read_sql):
        """Test data grabber with query error"""
        # Mock pandas read_sql to raise an exception
        mock_read_sql.side_effect = Exception("SQL Error: syntax error")
        
        mock_engine = MagicMock()
        
        # Test that the exception is re-raised
        with self.assertRaises(Exception) as context:
            dataGrabber("INVALID SQL", mock_engine)
        
        self.assertIn("SQL Error", str(context.exception))
    
    def test_dataGrabber_with_limit(self):
        """Test data grabber with limit parameter"""
        with patch('pandas.read_sql') as mock_read_sql, \
             patch('pg_helpers.database.play_notification_sound'), \
             patch('time.time', side_effect=[0, 1]):
            
            mock_df = pd.DataFrame({'col1': [1]})
            mock_read_sql.return_value = mock_df
            mock_engine = MagicMock()
            
            # Test with limit
            query = "SELECT * FROM test FOR READ ONLY"
            result = dataGrabber(query, mock_engine, limit='10')
            
            # Verify the query was modified to include LIMIT
            expected_query = "SELECT * FROM test LIMIT 10 FOR READ ONLY"
            mock_read_sql.assert_called_once_with(expected_query, mock_engine)


class TestIntegration(unittest.TestCase):
    """Integration tests that test multiple components together"""
    
    def test_query_workflow(self):
        """Test a typical query workflow"""
        # Create a test SQL file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql') as f:
            f.write("SELECT * FROM users WHERE id IN ($USER_IDS) AND created_date >= $START_DATE")
            temp_file = f.name
        
        try:
            # Test the complete workflow
            user_ids = [100, 200, 300]
            cleaned_query = queryCleaner(
                file=temp_file,
                list1=user_ids,
                varString1='$USER_IDS',
                startDate='2023-01-01',
                endDate='2023-12-31'
            )
            
            expected = "SELECT * FROM users WHERE id IN (100,200,300) AND created_date >= '2023-01-01'"
            self.assertEqual(cleaned_query, expected)
            
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)

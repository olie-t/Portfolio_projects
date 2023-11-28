import unittest
import logging
import sys
from io import StringIO
import unittest.mock as mock
from unittest.mock import MagicMock, mock_open, patch


import pandas as pd
from Subscriber_Pipeline_Functions import (setup_logger, connect_to_database, extract_and_transform_data,
                                           intialize_output_db, compare_and_update_data, generate_csv)

class LoggerTestClass(unittest.TestCase):
    def setUp(self):
        self.log_file = "test_log.log"
        self.changelog_file = "test_changelog.log"
        self.original_stdout = sys.stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()

    def tearDown(self):
        sys.stdout = self.original_stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()

    def test_setup_logger(self):
        captured_output = StringIO()
        sys.stdout = captured_output
        logger_name = 'test_setup_logger'
        logger = logging.getLogger(logger_name)
        while logger.handlers:
            logger.removeHandler(logger.handlers[0])
        test_logger = setup_logger(self.log_file, self.changelog_file, logger_name=logger_name)

        self.assertEqual(test_logger.level, logging.DEBUG)
        for handler in test_logger.handlers:
            print(type(handler))
        self.assertEqual(len(test_logger.handlers), 3)

        test_logger.info("Test info message")
        test_logger.warning("Test warning message")

        self.assertIn("Test info message", captured_output.getvalue())

        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn("Test info message", log_contents)

        with open(self.changelog_file, 'r') as changelog_file:
            changelog_contents = changelog_file.read()
            self.assertIn("Test warning message", changelog_contents)



class TestClass(unittest.TestCase):

    def setUp(self):
        self.log_file = "test_log.log"
        self.changelog_file = "test_changelog.log"
        self.original_stdout = sys.stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()
        self.logger = setup_logger(self.log_file, self.changelog_file)
        self.conn = connect_to_database(':memory:', self.logger)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        sys.stdout = self.original_stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()
        self.cursor.close()
        self.conn.close()

    def test_connect_to_database(self):

        self.cursor.execute("SELECT 1")
        result = self.cursor.fetchone()

        self.assertEqual(result, (1,))
        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn('Connection to database established', log_contents)

    def test_extract_and_transform_data(self):

        self.cursor.execute("""
            CREATE TABLE cademycode_students(
                uuid INTEGER PRIMARY KEY,
                name TEXT,
                dob TEXT,
                sex TEXT,
                contact_info TEXT,
                job_id TEXT,
                num_course_taken INTEGER,
                current_career_path_id INTEGER,
                time_spent_hrs INTEGER)""")
        self.cursor.execute("""
            CREATE TABLE cademycode_student_jobs (
                job_id INTEGER PRIMARY KEY,
                job_catagory TEXT,
                avg_salary INTEGER)""")
        self.cursor.execute("""
            CREATE TABLE cademycode_courses (
                career_path_id INTEGER PRIMARY KEY,
                career_path_name TEXT,
                hours_to_complete INTEGER)""")
        self.conn.commit()
        self.cursor.execute("""
            INSERT INTO cademycode_students VALUES(1, 'John Doe', '1900-01-01', 'M', '1234567890', 1, 3, 1, 100)""")
        self.cursor.execute("""
            INSERT INTO cademycode_student_jobs VALUES (1, 'Software Engineer', 80000)""")
        self.cursor.execute("""
            INSERT INTO cademycode_courses VALUES (1, 'Software Development', 200 )""")
        self.conn.commit()

        wide_df = extract_and_transform_data(self.cursor, self.logger)
        self.assertIsNotNone(wide_df, "wide dataframe not created")
        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn('Cleaned null values from job entries', log_contents)
            self.assertIn('Intial DataFrames created', log_contents)
            self.assertIn('Wide dataframe created and prepared', log_contents)

        test_df = pd.DataFrame(columns=['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken',
                                        'current_career_path_id', 'time_spent_hrs', 'career_path_name',
                                        'hours_to_complete'])
        assert test_df.shape[1] == wide_df.shape[1], "Dataframes have different shapes"

    def test_intialize_output_db_exists(self):
        self.cursor.execute("""
                CREATE TABLE students_analysis (
                    uuid integer,
                    name text,
                    dob text,
                    sex text,
                    contact_info text, 
                    job_id integer,
                    num_course_taken integer,
                    current_career_path_id integer,
                    time_spent_hrs integer,
                    career_path_name text,
                    hours_to_complete integer)
                """)
        self.conn.commit()

        result_existing_table = intialize_output_db(self.conn, self.cursor, self.logger)
        self.assertEqual(result_existing_table, [], "Expected empty list for existing table")

        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn("Table in output exists, gathering data", log_contents)

    def test_intialize_output_db_not_exists(self):

        result_new_table = intialize_output_db(self.conn, self.cursor, self.logger)
        self.assertEqual(result_new_table, [], "Expected empty list for new table")

        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn('Table in out put dosent exist....Creating Table ', log_contents)

class TestCompareAndUpdateData(unittest.TestCase):
    def setUp(self):
        self.log_file = "test_log.log"
        self.changelog_file = "test_changelog.log"
        self.original_stdout = sys.stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()

        # Set up database connection and logger
        self.logger = setup_logger(self.log_file, self.changelog_file)
        self.conn = connect_to_database(':memory:', self.logger)
        self.cur = self.conn.cursor()

        # Set up database tables and initial data
        self.cur.execute("""
                    CREATE TABLE students_analysis (
                        uuid integer,
                        name text,
                        dob text,
                        sex text,
                        contact_info text, 
                        job_id integer,
                        num_course_taken integer,
                        current_career_path_id integer,
                        time_spent_hrs integer,
                        career_path_name text,
                        hours_to_complete integer)
                    """)
        self.conn.commit()

        # Create sample data for testing
        self.wide_df = pd.DataFrame({
            'uuid': [1, 2],
            'name': ['Alice', 'Bob'],
            'dob': ['2023-06-03', '2023-07-04'],
            'sex': ['F', 'M'],
            'contact_info': [{"mailing_address": "303 N Timber Key, Irondale, Wisconsin, 84736", "email": "annabelle_avery9376@woohoo.com"}
                , {"mailing_address": "767 Crescent Fair, Shoals, Indiana, 37439", "email": "rubio6772@hmail.com"} ],
            'job_id': [5, 7],
            'num_course_taken': [6.0, 6.0],
            'current_career_path_id': [5.0, 5.0],
            'time_spent_hrs': [5.99, 3.6],
            'career_path_name': ['data scientist', 'data analyst'],
            'hours_to_complete': [20.0, 35.0]
        })
        self.wide_df['contact_info'] = self.wide_df['contact_info'].astype(str)
    def tearDown(self):
        sys.stdout = self.original_stdout
        open(self.log_file, 'w').close()
        open(self.changelog_file, 'w').close()
        self.conn.close()

    def test_compare_and_update_db_success(self):
        self.cur.executemany("""INSERT INTO students_analysis (uuid, name, dob, sex, contact_info, job_id, 
                                    num_course_taken, current_career_path_id, time_spent_hrs, career_path_name,
                                    hours_to_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             [(1, 'Alice', '1943-07-03', 'f',
                            '{"mailing_address": "767 Crescent Fair, Shoals, Indiana, 37439"}', 7, 6.0, 1.0, 4.99,
                            'data scientist', 22.0),
                            (3, 'Charlie', ' 1991-01-07', 'M',
                             '{"mailing_address": "767 Crescent Fair, Shoals, Indiana, 37439"}', 7, 6.0, 1.0, 4.99,
                             'data scientist', 22.5)])
        self.conn.commit()

        result_df = compare_and_update_data(self.wide_df, self.cur, self.conn, self.logger)
        self.assertIsNotNone(result_df)

        with open(self.log_file, 'r') as log_file:
            log_contents = log_file.read()
            self.assertIn('Comparison dataframe completed', log_contents)

    def test_compare_and_update_db_failure(self):
        with patch('sqlite3.connect') as mock_connect:
            mock_connect.return_value.cursor.return_value.execute.side_effect = Exception("Database query failed")

            logger = setup_logger(self.log_file, self.changelog_file)
            conn = connect_to_database(':memory:', logger)
            cur = conn.cursor()

            result_df = compare_and_update_data(self.wide_df, cur, conn, logger)

            self.assertIsNone(result_df)
            with open(self.log_file, 'r') as log_file:
                log_contents = log_file.read()
                self.assertIn("Creation or comparison of analysis failed", log_contents)

    def test_generate_csv_success(self):
        mock_logger = MagicMock()
        with patch('builtins.open', mock_open()) as mocked_file:
            result = generate_csv(self.wide_df, 'test.csv', mock_logger)

        self.assertTrue(result)
        mock_logger.info.assert_called_with(f"Data exported to CSV, {len(self.wide_df)} rows submitted.")
        mocked_file.assert_called_with("test.csv", "w")

    def test_generate_csv_file_content(self):
        mock_logger = MagicMock()
        expected_csv_content = self.wide_df.to_csv(index=False)

        with patch('builtins.open', mock_open()) as mocked_file:
            generate_csv(self.wide_df, 'test.csv', mock_logger)
            written_content = ''.join(call_args[0][0] for call_args in mocked_file.return_value.write.call_args_list)
            self.assertEqual(written_content, expected_csv_content)



if __name__ == '__main__':
    unittest.main()

import logging
import sys
import sqlite3
import pandas as pd


def setup_logger(log_file, changelog_file):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    change_handler = logging.FileHandler(changelog_file, encoding="utf-8")
    change_handler.setLevel(logging.WARNING)
    change_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(change_handler)
    logger.addHandler(stream_handler)
    return logger

def connect_to_database(db_path, logger):
    try:
        con = sqlite3.connect(db_path)
        logger.info("Connection to database established")
        return con

    except Exception as e:
        logger.error(f"Failed to connect to DB with error:\n{e}")
        return None

def extract_and_transform_data(cur, logger):
    try:
        students = cur.execute("SELECT * FROM cademycode_students")
        students_df = pd.DataFrame(students,
                               columns=['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken',
                                        'current_career_path_id', 'time_spent_hrs'])
        students_df.set_index(["uuid"])
        students_jobs = cur.execute("SELECT * FROM cademycode_student_jobs")
        students_jobs_df = pd.DataFrame(students_jobs, columns=['job_id', 'job_catagory', 'avg_salary'])
        courses = cur.execute("SELECT * FROM cademycode_courses")
        courses_df = pd.DataFrame(courses, columns=['career_path_id', 'career_path_name', 'hours_to_complete'])

        students_df["job_id"] = students_df["job_id"].fillna(99)
        students_jobs_df.loc[len(students_jobs_df)] = [99, 'N/A', 'N/A']
        logger.info("Cleaned null values from job entries")
        students_float_df = students_df.astype({"job_id": "float", "current_career_path_id": "float"})
        students_int_df = students_float_df.astype({"job_id": "Int64", "current_career_path_id": "float"})
        logger.info("Intial DataFrames created")


    except Exception as e:
        logger.error(f"Failed to extract data with error:\n{e}")
        return None

    try:
        wide_df_prep = pd.merge(students_int_df, students_jobs_df, on="job_id")
        wide_df_prep = pd.merge(students_int_df, courses_df, left_on="current_career_path_id",
                                right_on="career_path_id",
                                how="left")
        wide_df = wide_df_prep[["uuid", "name", "dob", "sex", "contact_info", "job_id", "num_course_taken",
                                "current_career_path_id", "time_spent_hrs", "career_path_name",
                                "hours_to_complete"]].copy()
        wide_df = wide_df.astype({"num_course_taken": "Float64", "time_spent_hrs": "Float64"})
        wide_df.set_index(["uuid"])
        logger.info("Wide dataframe created and prepared")
        return wide_df

    except Exception as e:
        logger.error(f"Failed to create wide_df with error:\n{e}")
        return None

def connect_output_db(output_db, logger):
    try:
        con1 = sqlite3.connect(output_db)
        logger.info("Connection to output database established")
        return con1
    
    except Exception as e:
        logger.error("Connection to output database failed with error:\n{e}")
        return None

def intialize_output_db(con, cur, logger):
    try:
        students_analysis = cur.execute("SELECT * FROM students_analysis").fetchall()
        logger.info("Table in output exists, gathering data")
        return students_analysis

    except sqlite3.OperationalError:
        logger.error(f"Table in out put dosent exist....Creating Table ")
        cur.execute("""
                            CREATE TABLE students_analysis(
                            uuid integer,
                            name object,
                            dob object,
                            sex object,
                            contact_info object,
                            job_id Int64,
                            num_course_taken object,
                            current_career_path_id float64,
                            time_spent_hrs object,
                            career_path_name object,
                            hours_to_complete float64)
                        """)
        con.commit()
        students_analysis = cur.execute("SELECT * FROM students_analysis").fetchall()
        logger.info("Empty dataframe prepared for analysis")
        return students_analysis

    except Exception as e:
        logger.error(f"Querying output DB failed with error:\n {e}")
        return None

def compare_and_update_data(wide_df, cur, con, logger):
    try:
        students_analysis = cur.execute("SELECT * FROM students_analysis").fetchall()
        logger.info("Gathering existing data from output.")
        students_analysis_df = pd.DataFrame(students_analysis,
                                        columns=["uuid", "name", "dob", "sex", "contact_info", "job_id",
                                                 "num_course_taken", "current_career_path_id", "time_spent_hrs",
                                                 "career_path_name", "hours_to_complete"])
        students_analysis_df.set_index(["uuid"])
        logger.info("Analysis dataframe created.")
        logger.info("Comparing input and output database information to find variance")
        wide_diff_df = pd.concat([wide_df, students_analysis_df]).drop_duplicates(keep=False)
        logger.info("Comparison dataframe completed")
    except Exception as e:
        logger.error(f"Creation or comparison of analysis failed with error:\n{e}")

    try:
        wide_diff_df.to_sql("students_analysis", con, if_exists="append", chunksize=50, index=False, dtype=
        {"uuid": "integer", "name": "object", "dob": "object", "sex": "object", "contact_info": "object",
         "job_id": "Int64",
         "num_course_taken": "object", "current_career_path": "Float64", "time_spent_hrs": "object",
         "career_path_name": "object", "hours_to_complete": "Float62"})
        con.commit
        logger.warning(f"Pushed {str(wide_diff_df.shape[0])} rows to output DB successfully")
        return wide_diff_df

    except Exception as e:
        logger.error(f"Pipeline failed to push data to output DB with error:\n{e}")
        return None

def generate_csv(data, csv_file, logger):
    try:
        with open(csv_file, "w") as file:
            data.to_csv(file)
        logger.info(f"Data exported to CSV, {len(data)} rows submitted.")
        return True
    except Exception as e:
        logger.error(f"Failed to generate CSV with error:\n{e}")
        return None


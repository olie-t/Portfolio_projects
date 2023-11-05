import Subscriber_Pipeline_Functions as sp

def pipeline(db, output_file, output_db, log, changelog):
    logger = sp.setup_logger(log, changelog)
    logger.info("***NEW RUN STARTED***")

    input_con = sp.connect_to_database(db, logger)
    output_con = sp.connect_to_database(output_db, logger)

    if not input_con or not output_con:
        return

    input_cur = input_con.cursor()
    output_cur = output_con.cursor()

    wide_df = sp.extract_and_transform_data(input_cur,logger)
    if wide_df is None:
        return

    if sp.intialize_output_db(output_con, output_cur, logger) is None:
        return

    final_data = sp.compare_and_update_data(wide_df, output_cur, output_con, logger)

    if final_data is None:
        return

    sp.generate_csv(final_data, output_file, logger)

    input_con.close()
    output_con.close()


db = r"C:\Users\oltho\Documents\Data Engineer Career Path (Code Academy)\DE Portfolio Project\dev\dev\cademycode_updated.db"
csv = r"C:\Users\oltho\Documents\Data Engineer Career Path (Code Academy)\DE Portfolio Project\dev\dev\subscriber_pipleline.csv"
log = r"C:\Users\oltho\Documents\Data Engineer Career Path (Code Academy)\DE Portfolio Project\dev\dev\subscriber_pipleline_log.txt"
output_db = r"C:\Users\oltho\Documents\Data Engineer Career Path (Code Academy)\DE Portfolio Project\dev\dev\output.db"
change_log = r"C:\Users\oltho\Documents\Data Engineer Career Path (Code Academy)\DE Portfolio Project\dev\dev\subscriber_change_log.txt"
pipeline(db, csv, output_db, log, change_log)
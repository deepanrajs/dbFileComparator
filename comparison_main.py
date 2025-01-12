import logging
import os.path
import sys

import configReader
import csv_vs_csv
import csv_vs_db
import db_vs_db
import time
import datetime


def main():
    # Main function to load the configuration and call the appropriate comparison function.

    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
    output_directory = './Output/Comparison_Report_' + formatted_datetime

    config_file = './config/config.ini'  # Path to your INI config file
    log_file_name = configReader.configure_logging(output_directory, formatted_datetime)
    config = configReader.load_config(config_file)

    # Get comparison type from the config
    source = config.get('comparison','source')
    target = config.get('comparison','target')

    # Call respective functions based on source and target
    if source.upper().startswith('CSV_') and target.upper().startswith('CSV_'):
        print('\tComparing CSV vs. CSV...')
        # csv_vs_csv.comparison(config)
        csv_vs_csv.comparison(config,output_directory)
    elif source.upper().startswith('DB_') and target.upper().startswith('DB_'):
        print('\tComparing DB vs. DB')
        db_vs_db.comparison(config,output_directory)
    elif ((source.upper().startswith('CSV_') and target.upper().startswith('DB_'))
          or (source.upper().startswith('DB_') and target.upper().startswith('CSV_'))):
        print('\tComparing CSV vs. DB')
        csv_vs_db.comparison(config,output_directory)
    else:
        sys.exit('Unsupported comparison type. Please check the config.')
    return log_file_name


if __name__ == '__main__':
    try:
        start_time = time.time()
        log_file = main()
        end_time = time.time()
        time_taken = end_time - start_time
        hours, remainder = divmod(time_taken, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = int((seconds - int(seconds)) * 1000)
        print(f"\nTime taken: {int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):02}")
        print(f"\nLog file saved at: {os.path.abspath(log_file)}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        pass
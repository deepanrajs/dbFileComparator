import configReader
import csv_vs_csv
import csv_vs_db
import db_vs_db
import time


def main():
    # Main function to load the configuration and call the appropriate comparison function.

    config_file = 'config.ini'  # Path to your INI config file
    config = configReader.load_config(config_file)

    # Get comparison type from the config
    source = config.get('comparison','source')
    target = config.get('comparison','target')

    # Call respective functions based on source and target
    if source.upper().startswith('CSV_') and target.upper().startswith('CSV_'):
        print('\tComparing CSV vs. CSV...')
        csv_vs_csv.comparison(config)
    elif source.upper().startswith('DB_') and target.upper().startswith('DB_'):
        print('\tComparing DB vs. DB')
        db_vs_db.comparison(config)
    elif ((source.upper().startswith('CSV_') and target.upper().startswith('DB_'))
          or (source.upper().startswith('DB_') and target.upper().startswith('CSV_'))):
        print('Comparing CSV vs. DB')
        csv_vs_db.comparison(config)
    else:
        print('Unsupported comparison type. Please check the config.')


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    time_taken = end_time - start_time
    hours, remainder = divmod(time_taken, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    print(f"\nTime taken: {int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):02}")
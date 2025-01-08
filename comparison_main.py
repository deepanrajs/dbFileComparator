import configReader
import csv_vs_csv
import csv_vs_db
import db_vs_db


def main():
    # Main function to load the configuration and call the appropriate comparison function.

    config_file = 'config.ini'  # Path to your INI config file
    config = configReader.load_config(config_file)

    # Get comparison type from the config
    comparison = config.get('comparison','comparison')

    # Call respective functions based on source and target
    if comparison.upper() == 'CSV_VS_CSV':
        print('\tComparing CSV vs. CSV...')
        csv_vs_csv.comparison(config)
    elif comparison.upper() == 'DB_VS_DB':
        print('\tComparing DB vs. DB')
        db_vs_db.comparison(config)
    elif comparison.upper() == 'DB_VS_CSV' or comparison.upper() == 'CSV_VS_DB':
        print('Comparing CSV vs. DB')
        csv_vs_db.comparison(config)
    else:
        print('Unsupported comparison type. Please check the config.')


if __name__ == '__main__':
    main()

import csv

import csv_vs_csv
import mssqlConnect


def comparison(config, output_directory):
    _, s_section = config['comparison']['source'].split('_', 1)
    _, t_section = config['comparison']['target'].split('_', 1)
    s_file = config.get(s_section, 'table_name')
    t_file = config.get(s_section, 'table_name')
    s_key = config.get(s_section, 'key')
    t_key = config.get(t_section, 'key')
    s_delimiter = config.get(s_section, 'delimiter')
    t_delimiter = config.get(t_section, 'delimiter')
    s_columns_excluded = config.get(s_section, 'columns_excluded')
    t_columns_excluded = config.get(t_section, 'columns_excluded')
    html_report = config.get('comparison', 'html_report')
    extended_report = config.get('comparison', 'extended_report')
    isFeeder = config.get('comparison', 'isFeeder')
    if isFeeder.upper() == 'Y':
        print('\n\t\tLookup File Comparison')
        feederFileName = config.get('comparison', 'feederFileName')
        feederFileDelimiter = config.get('comparison', 'feederFileDelimiter')
        print('\t\t\tFeeder File Name: ', feederFileName, feederFileDelimiter)
        with open('./config/'+feederFileName, 'r') as f:
            reader = list(csv.DictReader(f, delimiter=feederFileDelimiter))
            total_lines = len(reader)
            print(f'\t\t\t\tTotal entries in the feeder file: {total_lines}')
            # Count lines where Compare = Y
            flaggedFiles = sum(1 for row in reader if row['Compare'].strip().upper() == 'Y')
            print(f'\t\t\tProceeding with comparison for {flaggedFiles} selected entries.')
            counter = 0
            for idx, row in enumerate(reader, start=1):
                if None in row:
                    del row[None]
                compare = row['Compare'].strip().lower()
                counter += 1
                if compare.upper() == 'Y':
                    print("\n\nComparing Lookup Row: ", counter)
                    s_file = row['Source'].strip()
                    s_key = row['Source_Key'].strip()
                    s_delimiter = row['Source_Delimiter'].strip()
                    s_columns_excluded = row['columns_excluded'].strip().lower()
                    t_file = row['Target'].strip()
                    t_key = row['Target_Key'].strip()
                    t_delimiter = row['Target_Delimiter'].strip()
                    t_columns_excluded = row['columns_excluded'].strip()
                    s_file_path = mssqlConnect.connect(config, s_section, s_file, output_directory, exportAsFile='Y')
                    t_file_path = mssqlConnect.connect(config, t_section, t_file, output_directory, exportAsFile='Y')
                    print('\tData Extracted.')
                    csv_vs_csv.compare_csv(s_file_path,t_file_path,s_key,t_key,s_delimiter,t_delimiter,
                                           s_columns_excluded, t_columns_excluded,html_report,extended_report,
                                           output_directory,'N',str(idx),'')
                    


    elif isFeeder.upper != 'Y':
        s_file_path = mssqlConnect.connect(config, s_section, s_file, output_directory, exportAsFile='Y')
        t_file_path = mssqlConnect.connect(config, t_section, t_file, output_directory, exportAsFile='Y')
        csv_vs_csv.compare_csv(s_file_path, t_file_path, s_key, t_key, s_delimiter, t_delimiter,
                               s_columns_excluded, t_columns_excluded, html_report, extended_report, output_directory,
                               'N','','')

import os

import pandas as pd
import numpy as np
import datetime

import reportHTML

current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
output_directory = './Output/Comparison_Report_' + formatted_datetime
os.makedirs(output_directory, exist_ok=True)


def get_key(key):
    if type(key) is tuple:
        final_key = ""
        for value in key:
            final_key = final_key + "_" + str(value)
        return final_key[1:len(final_key) - 1]
    else:
        return str(key)

def comparison(config):
    s_section = config['comparison']['source']
    t_section = config['comparison']['target']
    s_file = config[s_section]['file_path']
    t_file = config[t_section]['file_path']
    s_key = config[s_section]['key']
    t_key = config[t_section]['key']
    s_delimiter = config[s_section]['delimiter']
    t_delimiter = config[t_section]['delimiter']
    s_columns_excluded = config[s_section]['columns_excluded']
    t_columns_excluded = config[s_section]['columns_excluded']
    html_report = config['compare']['html_report']
    extended_report = config['compare']['extended_report']

    print('Data: ', s_section, t_section, s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, html_report, extended_report)
    print(os.path.abspath(s_file))
    print(os.path.abspath(t_file))
    source_data = pd.read_csv(os.path.abspath(s_file), index_col=s_key, sep=s_delimiter, encoding_errors='ignore')
    target_data = pd.read_csv(os.path.abspath(t_file), index_col=t_key, sep=t_delimiter, encoding_errors='ignore')
    source_data = source_data.replace(np.nan, '')
    target_data = target_data.replace(np.nan, '')
    source_record_count = len(source_data.axes[0])
    target_record_count = len(target_data.axes[0])
    s_columns = []
    t_columns = []
    ext_report = open(output_directory + '\\' + extended_report, 'w')
    ext_report_abs = os.path.abspath(output_directory + '\\' + extended_report)
    ext_report.write("KEY, COLUMN, SOURCE_TABLE, SOURCE_VALUE, TARGET_TABLE, TARGET_VALUE, COMMENTS" + "\n")
    for column in source_data.columns:
        if column not in s_columns_excluded:
            s_columns.append(column)
    for column in target_data.columns:
        if column not in t_columns_excluded:
            t_columns.append(column)

    source_keys = set(source_data.index)  # Convert source keys to a set
    target_keys = set(target_data.index)  # Convert target keys to a set

    matched_keys = source_keys & target_keys  # Intersection of source and target keys (matched)
    unmatched_source_keys = source_keys - matched_keys  # Source keys that do not match
    unmatched_target_keys = target_keys - matched_keys  # Target keys that do not match

    # Initialize counters
    matched_records = 0
    mismatched_records = 0
    records_in_source_only = len(unmatched_source_keys)
    records_in_target_only = len(unmatched_target_keys)

    # Compare matched keys row by row and column by column
    for key in matched_keys:
        s_row = source_data.loc[key]
        t_row = target_data.loc[key]
        is_matched = True  # Assume matched unless we find a mismatch

        # Compare each column in the matched rows
        for column in s_columns:
            if column in t_columns:
                if s_row[column] != t_row[column]:  # If values don't match
                    mismatched_records += 1
                    is_matched = False
                    ext_report.write(get_key(key) + "," + str(column) + "," +
                                     s_file + "," + str(s_row[column]) + "," +
                                     t_file + "," + str(t_row[column]) + ",Mismatch\n")

        if is_matched:
            matched_records += 1

    # Process unmatched records (source only and target only)
    # Source-only records (present in source but not in target)
    for s_index in unmatched_source_keys:
        s_row = source_data.loc[s_index]
        ext_report.write(get_key(s_index) + "," + "," +
                         s_file + ",," + t_file + ",," + "Missing in Target\n")
        # for column in s_columns:
        #     ext_report.write(get_key(s_index) + "," + str(column) + "," +
        #                      s_file + "," + str(s_row[column]) + "," + "\n")

    # Target-only records (present in target but not in source)
    for t_index in unmatched_target_keys:
        t_row = target_data.loc[t_index]
        ext_report.write(get_key(t_index) + "," + "," +
                         s_file + ",," + t_file + ",," + "Missing in Source\n")
        # for column in t_columns:
        #     ext_report.write(get_key(t_index) + "," + str(column) + "," + "," + str(t_row[column]) + ",,\n")


    ext_report.close()
    print("\nSource_record_count " + str(source_record_count))
    print("Target_record_count " + str(target_record_count))
    print("\nMatched_records " + str(matched_records))
    print("Mismatched_records " + str(mismatched_records))
    print("\nRecords_in_source_only " + str(records_in_source_only))
    print("Records_in_target_only " + str(records_in_target_only))
    print("ext_report_abs: ", ext_report_abs)
    reportHTML.create_html_report(source_record_count, target_record_count, matched_records, mismatched_records,
                                  records_in_source_only, records_in_target_only, html_report,output_directory,
                                  s_file, t_file, ext_report_abs)

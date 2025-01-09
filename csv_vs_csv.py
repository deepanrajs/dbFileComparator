import csv
import datetime
import os

import numpy as np
import pandas as pd

import reportHTML

current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
output_directory = './Output/Comparison_Report_' + formatted_datetime


def get_key(key):
    if type(key) is tuple:
        final_key = ''
        for value in key:
            final_key = final_key + '_' + str(value)
        return final_key[1:len(final_key) - 1]
    else:
        return str(key)


def comparison(config):
    _,s_section = config['comparison']['source'].split('_', 1)
    _,t_section = config['comparison']['target'].split('_', 1)
    s_file = config[s_section]['file_path']
    t_file = config[t_section]['file_path']
    s_key = config[s_section]['key']
    t_key = config[t_section]['key']
    s_delimiter = config[s_section]['delimiter']
    t_delimiter = config[t_section]['delimiter']
    s_columns_excluded = config[s_section]['columns_excluded']
    t_columns_excluded = config[s_section]['columns_excluded']
    html_report = config['comparison']['html_report']
    extended_report = config['comparison']['extended_report']
    # Feeder Comparison
    isFeeder = config.get('comparison', 'isFeeder')
    if isFeeder.upper() == 'Y':
        feederFileName = config.get('comparison', 'feederFileName')
        feederFileDelimiter = config.get('comparison', 'feederFileDelimiter')
        print('\tFeeder File Name: ', feederFileName)
        with open(feederFileName, 'r') as f:
            reader = list(csv.DictReader(f, delimiter=feederFileDelimiter))
            total_lines = len(reader)
            print(f'\n\tTotal entries in the feeder file: {total_lines}')
            # Count lines where Compare = Y
            flaggedFiles = sum(1 for row in reader if row['Compare'].strip().upper() == 'Y')
            print(f'\t\tProceeding with comparison for {flaggedFiles} selected entries.')
            counter = 0
            for idx, row in enumerate(reader, start=1):
                if None in row:
                    del row[None]
                compare = row['Compare'].strip().lower()
                if compare.upper() == 'Y':
                    counter += 1
                    s_file = row['Source'].strip()
                    s_key = row['Source_Key'].strip()
                    s_delimiter = row['Source_Delimiter'].strip()
                    s_columns_excluded = row['columns_excluded'].strip().lower()
                    t_file = row['Target'].strip()
                    t_key = row['Target_Key'].strip()
                    t_delimiter = row['Target_Delimiter'].strip()
                    t_columns_excluded = row['columns_excluded'].strip()
                    print(f'\tComparing {counter} of {flaggedFiles}')
                    compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded,
                                t_columns_excluded, html_report, extended_report, isFeeder.upper(), str(idx))
    # CSV Folder comparison
    if isFeeder.upper() != 'Y' and os.path.isdir(s_file) and os.path.isdir(t_file):
        print('\nFolder comparison')
        source_files = {f: os.path.join(s_file, f) for f in os.listdir(s_file) if
                          f.endswith('.csv')}
        target_files = {f: os.path.join(t_file, f) for f in os.listdir(t_file) if f.endswith('.csv')}
        total_source = len(source_files)
        total_target = len(target_files)
        print(f'\tTotal files in Source/Target: {total_source}/{total_target}')
        comparison_count = 0  # Counter for successful comparisons
        total_comparisons = total_source  # Total number of files to compare (assuming one-to-one mapping)

        for source_file in source_files:
            for target_file in target_files:
                if source_file.strip() == target_file.strip():
                    comparison_count += 1  # Increment the successful comparison count
                    print(
                        f'\tComparing {comparison_count}/{total_comparisons} - source_file: {source_file} | target_file: {target_file}')
                    compare_csv(s_file+'/'+source_file, t_file+'/'+target_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded,
                                t_columns_excluded, html_report, extended_report,'',str(comparison_count), 'Y')

    if isFeeder.upper() != 'Y' and not os.path.isdir(s_file) and not os.path.isdir(t_file):
        print('\n\tIndividual File Comparison')
        compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded, t_columns_excluded,
                html_report, extended_report)


def compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded, t_columns_excluded,
                html_report, extended_report, isFeeder = 'N', counter = '', isFolderComparison = ''):
    try:

        source_data = pd.read_csv(os.path.abspath(s_file), sep=s_delimiter, encoding_errors='ignore',
                                  na_filter=True)
        target_data = pd.read_csv(os.path.abspath(t_file), sep=t_delimiter, encoding_errors='ignore',
                                  na_filter=True)
        source_data = source_data.replace(np.nan, '')
        target_data = target_data.replace(np.nan, '')

        if ',' in s_key and t_key:
            s_key_columns = s_key.split(',')
            t_key_columns = t_key.split(',')
            print('\tGenerating Composite Key')
            for col in s_key_columns:
                if col not in source_data.columns:
                    raise KeyError(f"Column '{col}' not found in source data.")
            for col in t_key_columns:
                if col not in target_data.columns:
                    raise KeyError(f"Column '{col}' not found in target data.")
            # Create composite keys for source and target data
            source_data['composite_key'] = source_data[s_key_columns].astype(str).agg('_'.join,
                                                                              axis=1)  # Combine columns in s_key
            target_data['composite_key'] = target_data[t_key_columns].astype(str).agg('_'.join,
                                                                              axis=1)  # Combine columns in t_key
            source_data.set_index('composite_key', inplace=True)
            target_data.set_index('composite_key', inplace=True)
        elif s_key == '' and t_key == '':
            sColumnList = list(source_data.columns)
            tColumnList = list(target_data.columns)
            source_data['composite_key'] = source_data[sColumnList].astype(str).agg('_'.join,
                                                                                      axis=1)  # Combine columns in s_key
            target_data['composite_key'] = target_data[tColumnList].astype(str).agg('_'.join,
                                                                                      axis=1)  # Combine columns in t_key
            source_data.set_index('composite_key', inplace=True)
            target_data.set_index('composite_key', inplace=True)
        else:
            source_data.set_index(s_key, inplace=True)
            target_data.set_index(t_key, inplace=True)

        source_record_count = len(source_data)
        target_record_count = len(target_data)
        s_columns = []
        t_columns = []
        os.makedirs(output_directory, exist_ok=True)
        fileName = os.path.basename(s_file) + ' vs. ' + os.path.basename(t_file)
        if isFeeder.upper() == 'Y' or isFolderComparison == 'Y':  #HERE
            fileName = counter + '_' + fileName
            ext_report = open(output_directory + '/' + fileName + '_' + extended_report, 'w')
        else:
            ext_report = open(output_directory + '/' + fileName + '_' + extended_report, 'w')
        ext_report_abs = os.path.abspath(output_directory + '/' + fileName + '_' + extended_report)
        ext_report.write('KEY, COLUMN, SOURCE_TABLE, SOURCE_VALUE, TARGET_TABLE, TARGET_VALUE, COMMENTS' + '\n')
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
                        ext_report.write(get_key(key) + ',' + str(column) + ',' +
                                         s_file + ',' + str(s_row[column]) + ',' +
                                         t_file + ',' + str(t_row[column]) + ',Mismatch\n')

            if is_matched:
                matched_records += 1

        # Process unmatched records (source only and target only)
        # Source-only records (present in source but not in target)
        for s_index in unmatched_source_keys:
            # s_row = source_data.loc[s_index]
            ext_report.write(get_key(s_index) + ',' + ',' +
                             s_file + ',,' + t_file + ',,' + 'Missing in Target\n')
            # for column in s_columns:
            #     ext_report.write(get_key(s_index) + ',' + str(column) + ',' +
            #                      s_file + ',' + str(s_row[column]) + ',' + '\n')

        # Target-only records (present in target but not in source)
        for t_index in unmatched_target_keys:
            # t_row = target_data.loc[t_index]
            ext_report.write(get_key(t_index) + ',' + ',' +
                             s_file + ',,' + t_file + ',,' + 'Missing in Source\n')
            # for column in t_columns:
            #     ext_report.write(get_key(t_index) + ',' + str(column) + ',' + ',' + str(t_row[column]) + ',,\n')

        ext_report.close()
        print('\t\tSource_record_count ' + str(source_record_count))
        print('\t\tTarget_record_count ' + str(target_record_count))
        print('\t\tMatched_records ' + str(matched_records))
        print('\t\tMismatched_records ' + str(mismatched_records))
        print('\t\tRecords_in_source_only ' + str(records_in_source_only))
        print('\t\tRecords_in_target_only ' + str(records_in_target_only))
        reportHTML.create_html_report(source_record_count, target_record_count, matched_records, mismatched_records,
                                      records_in_source_only, records_in_target_only, fileName+'_'+html_report, output_directory,
                                      s_file, t_file, ext_report_abs, counter)
    except FileNotFoundError as fnf_error:
        print(f'\nFile not found: {fnf_error}')

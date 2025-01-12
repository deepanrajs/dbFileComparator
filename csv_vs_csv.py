import csv
import os
import sys
import numpy as np
import pandas as pd

import reportHTML


def get_key(key):
    if type(key) is tuple:
        final_key = ''
        for value in key:
            final_key = final_key + '_' + str(value)
        return final_key[1:len(final_key) - 1]
    else:
        return str(key)

def comparison(config, output_directory):
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
        print('\n\t\tFeeder File Comparison')
        feederFileName = config.get('comparison', 'feederFileName')
        feederFileDelimiter = config.get('comparison', 'feederFileDelimiter')
        print('\t\t\tFeeder File Name: ', feederFileName)
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
                    print("\n\nComparing Feeder Row: ", counter)
                    s_file = row['Source'].strip()
                    s_key = row['Source_Key'].strip()
                    s_delimiter = row['Source_Delimiter'].strip()
                    s_columns_excluded = row['columns_excluded'].strip().lower()
                    t_file = row['Target'].strip()
                    t_key = row['Target_Key'].strip()
                    t_delimiter = row['Target_Delimiter'].strip()
                    t_columns_excluded = row['columns_excluded'].strip()
                    compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded,
                                t_columns_excluded, html_report, extended_report, output_directory, isFeeder,
                                str(idx), 'N')
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
                        f'\tComparing {comparison_count}/{total_comparisons} - '
                        f'source_file: {source_file} | target_file: {target_file}')
                    compare_csv(s_file+'/'+source_file, t_file+'/'+target_file, s_key, t_key, s_delimiter,
                                t_delimiter, s_columns_excluded, t_columns_excluded, html_report, extended_report,
                                output_directory,'N', str(comparison_count), 'Y')

    if isFeeder.upper() != 'Y' and not os.path.isdir(s_file) and not os.path.isdir(t_file):
        print('\n\t\tIndividual File Comparison')
        print(f'\t\t\tComparing Source: \'{s_file}\' and Target: \'{t_file}\'')
        compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded, t_columns_excluded,
                html_report, extended_report, output_directory,'Y','','')

def compare_csv(s_file, t_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded, t_columns_excluded,
                html_report, extended_report, output_directory, isFeeder = 'N', counter = '', isFolderComparison = ''):

    source_data = pd.read_csv(s_file, sep=s_delimiter, encoding_errors='ignore')
    target_data = pd.read_csv(t_file, sep=t_delimiter, encoding_errors='ignore')

    source_record_count = len(source_data)
    target_record_count = len(target_data)

    s_key_columns = [key.strip() for key in s_key.split('~')]
    t_key_columns = [key.strip() for key in t_key.split('~')]
    if len(s_key_columns) != len(t_key_columns):
        sys.exit(
            f'Mismatch in the number of columns for primary/composite keys. '
            f'Source key has {len(s_key_columns)} column(s), while Target key has {len(t_key_columns)} column(s).'
        )

    if '~' in s_key and '~' in t_key:
        # Handling composite keys when multiple keys are specified
        s_key_columns = [key.strip().lower() for key in s_key.split('~')]
        t_key_columns = [key.strip().lower() for key in t_key.split('~')]
        print('\n\t\tMultiple Primary Keys provided, generating Composite Keys')

        # Ensure all composite key columns exist in the source and target datasets
        for col in s_key_columns:
            if col not in [c.lower() for c in source_data.columns]:
                raise KeyError(f'Column {col} not found in source data.')
        for col in t_key_columns:
            if col not in [c.lower() for c in target_data.columns]:
                raise KeyError(f'Column {col} not found in target data.')

            # Normalize source and target column names to lower case for composite key creation
            source_data.columns = [col.lower() for col in source_data.columns]
            target_data.columns = [col.lower() for col in target_data.columns]

            # Create composite keys for source and target data
            source_data['composite_key'] = source_data[s_key_columns].astype(str).agg('_'.join, axis=1)
            target_data['composite_key'] = target_data[t_key_columns].astype(str).agg('_'.join, axis=1)

            # Use the composite keys as indices
            source_data.set_index('composite_key', inplace=True)
            target_data.set_index('composite_key', inplace=True)


    elif s_key == '' and t_key == '':
        print('\t\tNo Primary Keys provided, generating Composite Keys using all the columns')
        sColumnList = list(source_data.columns)
        tColumnList = list(target_data.columns)
        source_data['composite_key'] = source_data[sColumnList].astype(str).agg('_'.join, axis=1)
        target_data['composite_key'] = target_data[tColumnList].astype(str).agg('_'.join, axis=1)
        source_data.set_index('composite_key', inplace=True)
        target_data.set_index('composite_key', inplace=True)

    elif s_key and t_key:
        # Handling single primary key (fallback scenario)
        print('\t\tSingle Primary Key used for comparison')
        s_key = [key.strip().lower() for key in s_key.split(',')]
        t_key = [key.strip().lower() for key in t_key.split(',')]

        if len(s_key) != 1 or len(t_key) != 1:
            raise ValueError('Provide a single primary key for single-key comparison.')

        # Normalize source and target column names to lower case
        source_data.columns = [col.lower() for col in source_data.columns]
        target_data.columns = [col.lower() for col in target_data.columns]

        source_data.set_index(s_key[0], inplace=True)
        target_data.set_index(t_key[0], inplace=True)

        source_data['composite_key'] = source_data.index.astype(str)
        target_data['composite_key'] = target_data.index.astype(str)

    else:
        # If no keys are provided, fallback to row comparison
        sColumnList = list(source_data.columns)
        tColumnList = list(target_data.columns)

        source_data['composite_key'] = source_data[sColumnList].astype(str).agg('_'.join, axis=1)
        target_data['composite_key'] = target_data[tColumnList].astype(str).agg('_'.join, axis=1)

        source_data.set_index('composite_key', inplace=True)
        target_data.set_index('composite_key', inplace=True)

    source_data = source_data.replace(np.nan, '')
    target_data = target_data.replace(np.nan, '')

    matched_records = 0
    mismatched_records = 0

    s_columns = []
    t_columns = []

    os.makedirs(output_directory, exist_ok=True)
    fileName = os.path.basename(s_file) + ' vs. ' + os.path.basename(t_file)

    if isFeeder.upper() == 'Y' or isFolderComparison.upper() == 'Y':  # HERE
        fileName = counter + '_' + fileName
    abs_extended_report = output_directory + '\\'+ fileName + '_' +extended_report
    ext_report = open(abs_extended_report, 'w')
    ext_report.write('KEY, COLUMN, SOURCE_TABLE, SOURCE_VALUE, TARGET_TABLE, TARGET_VALUE, COMMENTS' + '\n')

    for scolumn in source_data.columns:
        if scolumn not in s_columns_excluded:
            s_columns.append(scolumn)
    for tcolumn in target_data.columns:
        if tcolumn not in t_columns_excluded:
            t_columns.append(tcolumn)

    source_keys = set(source_data.index)  # Convert source keys to a set
    target_keys = set(target_data.index)  # Convert target keys to a set

    matched_keys = source_keys & target_keys  # Intersection of source and target keys (matched)
    unmatched_source_keys = source_keys - matched_keys  # Source keys that do not match
    unmatched_target_keys = target_keys - matched_keys  # Target keys that do not match

    records_in_source_only = len(unmatched_source_keys)
    records_in_target_only = len(unmatched_target_keys)

    source_data['composite_key'] = source_data.index
    target_data['composite_key'] = target_data.index
    # Duplicate check for source and target composite keys


    source_duplicates_initial = source_data[source_data.duplicated(subset=['composite_key'], keep=False)]
    target_duplicates_initial = target_data[target_data.duplicated(subset=['composite_key'], keep=False)]
    source_duplicate_count = int(len(source_duplicates_initial)/2)
    target_duplicate_count = int(len(target_duplicates_initial)/2)

    if len(source_duplicates_initial)>0 or len(target_duplicates_initial)>0:
        print(f'\t\t\tDuplicate Records detected. '
              f'Executing fall back mechanism to remove duplicates if entire row is duplicated.')

        source_data = source_data.drop_duplicates()
        target_data = target_data.drop_duplicates()

        source_duplicates = source_data[source_data.duplicated(subset=['composite_key'], keep=False)]
        target_duplicates = target_data[target_data.duplicated(subset=['composite_key'], keep=False)]

        if len(source_duplicates)>0 or len(target_duplicates)>0:
            print(f'\n\t\t\tKey duplicates still identified. Attempted fallback mechanism to remove fully identical rows.'
                  f'\n\t\t\tHowever, duplicate keys with differing values still exist:'
                  f'\n\t\t\t\tSource Duplicates: {int(len(source_duplicates)/2)}'
                  f'\n\t\t\t\tTarget Duplicates: {int(len(target_duplicates)/2)}'
                  f'\n\t\t\tPlease ensure unique keys in your data before proceeding.')

            sys.exit('\nProgram terminated due to duplicate keys.')

    print('\nStarting data comparison...')
    for key in matched_keys:
        s_row = source_data.loc[key]
        t_row = target_data.loc[key]

        if not s_row.empty and not t_row.empty:
            row_mismatches = 0
            total_mismatches = 0
            for s_column in s_columns:
                for t_column in t_columns:
                    if s_column == t_column:
                        if s_row[s_column] != t_row[t_column]:
                            row_mismatches += 1  # Increment the mismatch count for this row
                            total_mismatches += 1  # Increment the overall mismatch count
                            ext_report.write(get_key(key) + ',' + str(s_column) + ',' +
                                             s_file + ',' + str(s_row[s_column]) + ',' +
                                             t_file + ',' + str(t_row[t_column]) + ', Mismatch' + '\n')
            if row_mismatches > 0:  # If there were any mismatches in the row
                mismatched_records += 1  # Count this row as a mismatched record
            else:
                matched_records += 1  # If no mismatches, it's a matched record

    for key in unmatched_target_keys:
        ext_report.write(f'{get_key(key)},,{s_file},,{t_file},,Missing in Source\n')

    for key in unmatched_source_keys:
        ext_report.write(f'{get_key(key)},,{s_file},,{t_file},,Missing in Target\n')

    ext_report.close()
    print('Comparison Stats: ')
    print('\tTotal Record Count: ')
    print('\t\t-> Source: ', source_record_count)
    print('\t\t-> Target: ', target_record_count)
    print('\tRecords with matching primary/composite keys across Source and Target: ', len(matched_keys))
    print('\t\t-> Matched Records: ', matched_records)
    print('\t\t-> Mismatched Records: ', mismatched_records)
    print('\tMissing Records: ')
    print('\t\t-> Records present only in Source: ', records_in_source_only)
    print('\t\t-> Records present only in Target: ', records_in_target_only)
    print('\tDuplicate Records')
    print('\t\t-> Source: ', source_duplicate_count)
    print('\t\t-> Target: ', target_duplicate_count)

    reportHTML.create_html_report(source_record_count,target_record_count,matched_records,mismatched_records,
                                  records_in_source_only, records_in_target_only,fileName+'_'+html_report,output_directory,
                                  s_file, t_file, os.path.abspath(abs_extended_report) ,'', source_duplicate_count,
                                  target_duplicate_count)
    return (source_record_count,target_record_count,matched_records,mismatched_records, records_in_source_only,
            records_in_target_only,fileName+'_'+html_report,output_directory,s_file, t_file,
            os.path.abspath(extended_report),'',source_duplicate_count,target_duplicate_count)

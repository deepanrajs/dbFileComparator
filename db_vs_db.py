import csv_vs_csv
import db2Connect
import mssqlConnect


def comparison(config):
    s_section = config['comparison']['source']
    t_section = config['comparison']['target']
    s_key = config.get(s_section,'key')
    t_key = config.get(t_section,'key')
    s_delimiter = config.get(s_section,'delimiter')
    t_delimiter = config.get(t_section,'delimiter')
    s_columns_excluded = config.get(s_section,'columns_excluded')
    t_columns_excluded = config.get(t_section,'columns_excluded')
    html_report = config.get('compare', 'html_report')
    extended_report = config.get('compare', 'extended_report')
    s_file_path = mssqlConnect.connect(config, s_section, exportAsFile='Y')
    t_file_path = mssqlConnect.connect(config, t_section, exportAsFile='Y')

    print(s_file_path, t_file_path)
    csv_vs_csv.compare_csv(s_file_path,t_file_path, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded,
                           t_columns_excluded, html_report, extended_report)

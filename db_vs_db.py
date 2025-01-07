import db2Connect


def comparison(config):
    s_section = config['comparison']['source']
    t_section = config['comparison']['target']

    s_file_path = db2Connect.connect(config, s_section, exportAsFile='Y')
    s_file_path = db2Connect.connect(config, t_section, exportAsFile='Y')

import configparser


def load_config(file_path):
    """
    Load the INI configuration file.
    :param file_path: Path to the config file
    :return: ConfigParser object
    """
    print('Reading Config...')
    config = configparser.ConfigParser()
    config.read(file_path)
    return config
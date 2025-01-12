import configparser
import logging
import os
import sys
from datetime import datetime


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

class LoggerWriter:
    def __init__(self, log_file):
        self.console = sys.stdout  # Keep the default console writer
        self.log = open(log_file, 'a')  # Open the log file in append mode

    def write(self, message):
        self.console.write(message)  # Write to the console
        self.log.write(message)  # Write to the log file

    def flush(self):
        self.console.flush()  # Ensure console messages are flushed
        self.log.flush()  # Ensure log file messages are flushed

def configure_logging(output_directory, formatted_datetime):
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Generate a unique log file name using a timestamp
    log_file = os.path.join(output_directory, f"Compare_log_{formatted_datetime}.log")

    # Set up logging configuration
    logging.basicConfig(
        level=logging.INFO,  # Default logging level
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),  # Log to file
            logging.StreamHandler()  # Log to console
        ]
    )

    # Redirect print statements to both console and log file
    sys.stdout = LoggerWriter(log_file)

    return log_file
import unittest
import sys
from io import StringIO
import csv_vs_csv
from contextlib import contextmanager
import datetime

current_datetime = datetime.datetime.now()
formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')


# Context manager to suppress console output
@contextmanager
def suppress_stdout():
    original_stdout = sys.stdout  # Save the original stdout
    sys.stdout = StringIO()  # Redirect stdout to a StringIO buffer
    try:
        yield
    finally:
        sys.stdout = original_stdout  # Restore original stdout


class TestCSVFeederComparator(unittest.TestCase):
    def test_comparator_function_without_feeder(self):
        # Suppress console output during the test
        with suppress_stdout():
            # Run the comparator function
            feeder_result = csv_vs_csv.compare_csv(
                './input/generated_data1_source.csv',  # Path to source CSV file
                './input/generated_data1_target.csv',  # Path to target CSV file
                'Row_ID_O6G3A1_R6,Order_ID',  # Keys for comparison
                'Row_ID_O6G3A1_R6,Order_ID',  # Keys for comparison
                ',',  # Delimiter for both files
                ',',  # Delimiter for both files
                '',  # Exclude columns (empty in this case)
                '',  # Exclude columns (empty in this case)
                'test_report.html',  # Report output file (HTML)
                'test_report.csv',  # Report output file (CSV)
                f'./Output/Comparison_Report_PyTest_{formatted_datetime}/Test1',  # Identical check (Yes)
                'N',  # isFeeder
                '',
                ''
            )

        # Assert that feeder_result is not None
        self.assertIsNotNone(feeder_result, "compare_csv returned None")

        # Extract values from the tuple
        source_record_count = feeder_result[0]
        target_record_count = feeder_result[1]
        matched_records = feeder_result[2]
        mismatched_records = feeder_result[3]
        records_in_source_only = feeder_result[4]
        records_in_target_only = feeder_result[5]
        source_duplicate_count = feeder_result[12]
        target_duplicate_count = feeder_result[13]

        # Add assertions for each field
        self.assertEqual(source_record_count, 12, "Source record count mismatch")
        self.assertEqual(target_record_count, 12, "Target record count mismatch")
        self.assertEqual(matched_records, 8, "Matched record count mismatch")
        self.assertEqual(mismatched_records, 2, "Mismatched record count mismatch")
        self.assertEqual(records_in_source_only, 1, "Records in source only count mismatch")
        self.assertEqual(records_in_target_only, 1, "Records in target only count mismatch")
        self.assertEqual(source_duplicate_count, 1, "Source duplicate count mismatch")
        self.assertEqual(target_duplicate_count, 1, "Target duplicate count mismatch")

    def test_comparator_function_with_feeder(self):
        # Suppress console output during the test
        with suppress_stdout():
            # Run the comparator function with isFeeder = 'Y'
            feeder_result = csv_vs_csv.compare_csv(
                './input/generated_data1_source.csv',  # Path to source CSV file
                './input/generated_data1_target.csv',  # Path to target CSV file
                'Row_ID_O6G3A1_R6,Order_ID',  # Keys for comparison
                'Row_ID_O6G3A1_R6,Order_ID',  # Keys for comparison
                ',',  # Delimiter for both files
                ',',  # Delimiter for both files
                '',  # Exclude columns (empty in this case)
                '',  # Exclude columns (empty in this case)
                'test_report.html',  # Report output file (HTML)
                'test_report.csv',  # Report output file (CSV)
                f'./Output/Comparison_Report_Pytest_{formatted_datetime}/Test2',  # Identical check (Yes)
                'Y',  # isFeeder
                '',
                ''
            )

        # Assert that feeder_result is not None
        self.assertIsNotNone(feeder_result, "compare_csv returned None")

        # Extract values from the tuple
        source_record_count = feeder_result[0]
        target_record_count = feeder_result[1]
        matched_records = feeder_result[2]
        mismatched_records = feeder_result[3]
        records_in_source_only = feeder_result[4]
        records_in_target_only = feeder_result[5]
        source_duplicate_count = feeder_result[12]
        target_duplicate_count = feeder_result[13]

        # Add assertions for each field
        self.assertEqual(source_record_count, 12, "Source record count mismatch")
        self.assertEqual(target_record_count, 12, "Target record count mismatch")
        self.assertEqual(matched_records, 8, "Matched record count mismatch")
        self.assertEqual(mismatched_records, 2, "Mismatched record count mismatch")
        self.assertEqual(records_in_source_only, 1, "Records in source only count mismatch")
        self.assertEqual(records_in_target_only, 1, "Records in target only count mismatch")
        self.assertEqual(source_duplicate_count, 1, "Source duplicate count mismatch")
        self.assertEqual(target_duplicate_count, 1, "Target duplicate count mismatch")



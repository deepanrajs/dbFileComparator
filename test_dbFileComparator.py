import unittest
import sys
from io import StringIO
import csv_vs_csv
from contextlib import contextmanager

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
    def test_comparator_function(self):
        # Suppress console output during the test
        with suppress_stdout():
            # Run the comparator function
            feeder_result = csv_vs_csv.compare_csv(
                './Input/source1.csv',
                './Input/target1.csv',
                'Row_ID_O6G3A1_R6',
                'Row_ID_O6G3A1_R6',
                '|', '|', '', '',
                'test_report.html', 'test_report.csv',
                'Y', '2', ''
            )

        # Assert that feeder_result is not None
        self.assertIsNotNone(feeder_result, "compare_csv returned None")

        # Extract the summary from the result
        comparison_summary = feeder_result.get("summary", {})

        # Add assertions for each field
        self.assertEqual(comparison_summary.get("source_record_count"), 6, "Source record count mismatch")
        self.assertEqual(comparison_summary.get("target_record_count"), 5, "Target record count mismatch")
        self.assertEqual(comparison_summary.get("matched_records"), 2, "Matched record count mismatch")
        self.assertEqual(comparison_summary.get("mismatched_records"), 1, "Mismatched record count mismatch")
        self.assertEqual(comparison_summary.get("records_in_source_only"), 1, "Records in source only count mismatch")
        self.assertEqual(comparison_summary.get("records_in_target_only"), 1, "Records in target only count mismatch")
        self.assertEqual(comparison_summary.get("source_duplicate_count"), 2, "Source duplicate count mismatch")
        self.assertEqual(comparison_summary.get("target_duplicate_count"), 1, "Target duplicate count mismatch")


if __name__ == '__main__':
    unittest.main()

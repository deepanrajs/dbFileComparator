import csv
import os.path

import pyodbc


def write_query_results(output_file, delimiter, q_result, header):
    try:
        with open(output_file, "w", newline='', encoding='utf-8') as file:
            # Write the header
            csv_writer = csv.writer(file, delimiter=delimiter)
            column_names = header
            csv_writer.writerow(column_names)
            csv_writer.writerows(q_result)
            print(f"\t\t\tData successfully written to {os.path.abspath(output_file)}")
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")


def connect(config, sectionHeader, tableName, output_directory, exportAsFile):
    delimiter = config.get(sectionHeader, 'delimiter')
    driver = config.get(sectionHeader, 'Driver')
    server = config.get(sectionHeader, 'Server')
    port = config.get(sectionHeader, 'Port')
    database = config.get(sectionHeader, 'dbname')
    Username = config.get(sectionHeader, 'Username')
    Password = config.get(sectionHeader, 'Password')
    try:
        myConnection = pyodbc.connect(
            f"Driver={driver};"
            f"Server={server},{port};"
            f"Database={database};"
            f"UID={Username};"
            f"PWD={Password};"
            "Encrypt=no;TrustServerCertificate=yes;"
        )
        cursor = myConnection.cursor()
        print("\t\t\tConnection established...")
        query = "SELECT * FROM " + tableName
        cursor.execute(query)
        query_result = cursor.fetchall()
        header = [description[0] for description in cursor.description]
        if exportAsFile == 'Y':
            out_file = f"./{output_directory}/{sectionHeader}_{tableName}.csv"
            if query_result is not None and query_result:
                write_query_results(out_file, delimiter, query_result, header)
                return out_file
            else:
                print("Query returned no results.")
                return None
        else:
            print(f"\nExtracting {sectionHeader} data from table {tableName}")
            return query_result
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

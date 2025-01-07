import csv
import pyodbc

def write_query_results(output_file, delimiter, q_result, header):
    try:
        with open(output_file, "w", newline='', encoding='utf-8') as file:
            # Write the header
            csv_writer = csv.writer(file, delimiter=delimiter)
            column_names = header
            csv_writer.writerow(column_names)
            csv_writer.writerows(q_result)
            print(f"Data successfully written to {output_file}")
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")


def connect(config, dbType, exportAsFile):
    global cursor, myConnection
    tablename = config.get(dbType, 'table_name')
    delimiter = config.get(dbType, 'delimiter')
    driver = config.get(dbType, 'Driver')
    server = config.get(dbType, 'Server')
    port = config.get(dbType, 'Port')
    database = config.get(dbType,'dbname')
    Username = config.get(dbType,'Username')
    Password = config.get(dbType,'Password')
    # Establish connection to SSMS
    # Connection string for QA instance.
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
        print("Connection established...")
        query = "SELECT * FROM " + tablename
        # print('query: ', query)
        cursor.execute(query)
        print("Query executed...")
        query_result = cursor.fetchall()
        # print("query_result: ", query_result)
        header = [description[0] for description in cursor.description]
        # print("header: ", header)
        if exportAsFile == 'Y':
            print(f"\nExporting {dbType} data to CSV")
            out_file = f"./Input/{dbType}_{tablename}.csv"
            if query_result is not None and query_result:
                write_query_results(out_file, delimiter, query_result, header)
                return out_file
            else:
                print("Query returned no results.")
                return None
        else:
            print(f"\nExtracting {dbType} data from table {tablename}")
            return query_result
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

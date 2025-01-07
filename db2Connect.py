import os

# Add DB2 driver path for Windows
os.add_dll_directory(
    'C:\\Users\\MITDeepanraj\\PycharmProjects\\dbFileComparator\\.venv\\Lib\\site-packages\\clidriver\\bin')
import ibm_db
import ibm_db_dbi as dbi


def connect(config, dbType, exportAsFile):
    try:
        # Fetch DB2 credentials from config
        hostname = config.get(dbType, 'hostname')
        port = config.get(dbType, 'port')
        username = config.get(dbType, 'username')
        password = config.get(dbType, 'password')
        dbname = config.get(dbType, 'dbname')
        tablename = config.get(dbType, 'table_name')
        delimiter = config.get(dbType, 'delimiter')

        # Check if required fields are present
        if not hostname or not port or not username or not password or not dbname:
            print("Verify DB2 details entered under [db2] section")
            return None

        # Create the connection string
        conn_string = (f"DATABASE={dbname};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;"
                       f"UID={username};PWD={password};")
        print('conn_string: ', conn_string)
        print(f"Connecting to DB2 database: {dbname} at {hostname}:{port}")

        print('# Perform DB2 operations')
        conn = ibm_db.connect(conn_string, username, password)
        print('conn: ', conn)

        # query_result = get_query_results(conn_string, tablename)
        query_result = get_query_results_dbi(conn, tablename)
        print('# End DB2 operations')

        if exportAsFile == 'Y':
            print(f"\nExporting {dbType} data to CSV")
            out_file = f"./Input/{dbType}_{tablename}.csv"
            if query_result is not None and query_result:
                write_query_results(out_file, delimiter, query_result)
                return out_file
            else:
                print("Query returned no results.")
                return None
        else:
            print(f"\nExtracting {dbType} data from table {tablename}")
            return query_result

    except Exception as e:
        print(f"Error during DB2 connection or data extraction: {e}")
        return None


def write_query_results(output_file, delimiter, q_result):
    try:
        with open(output_file, "w") as file:
            # Write the header
            header = q_result[0].keys()
            file.write(delimiter.join(header) + "\n")

            # Write the rows
            for row in q_result:
                file.write(delimiter.join(str(value) for value in row.values()) + "\n")
        print(f"Data successfully written to {output_file}")

    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")


def get_query_results(connection_string, table_name):
    try:
        # Connect to the DB2 database
        conn = ibm_db.connect(connection_string, '', '')
        print('conn: ', conn)
        if conn:
            query = f"SELECT * FROM {table_name}"
            stmt = ibm_db.exec_immediate(conn, query)
            ret = []

            # Fetch query results row by row
            result = ibm_db.fetch_assoc(stmt)
            while result:
                ret.append(result)
                result = ibm_db.fetch_assoc(stmt)

            # Close the connection
            ibm_db.close(conn)
            print(f"Query executed successfully on {table_name}, retrieved {len(ret)} rows.")
            return ret
        else:
            print(f"Connection failed with string: {connection_string}")
            return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def get_query_results_dbi(connection_string, table_name):  #CDIDTP
    try:
        # Establish a connection using ibm_db_dbi
        # conn = dbi.connect(connection_string, '', '')
        query = f"SELECT * FROM {table_name}"
        print('query: ', query)
        stmt = ibm_db.exec_immediate(connection_string, query)
        print('stmt: ', stmt)
        ret = []
        result = ibm_db.fetch_assoc(stmt)
        while result:
            ret.append(result)
            result = ibm_db.fetch_assoc(stmt)
        ibm_db.close(connection_string)
        return ret
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

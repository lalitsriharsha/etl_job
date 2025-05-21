from cassandra.query import SimpleStatement
from app.models.cassandra_setup import get_cassandra_session

session = get_cassandra_session()

def insert_into_cassandra(session, table_name, data_row):
    if not isinstance(data_row, dict):
        raise TypeError(f"Expected dict, got {type(data_row)}: {data_row}")

    # Define allowed columns based on table schema
    valid_columns = {
        "pid", "fname", "lname", "dob", "email", "gender", "cell",
        "city", "apartment", "street", "country", "pcode", "gestage"
    }

    filtered_row = {k: v for k, v in data_row.items() if k in valid_columns}

    if not filtered_row:
        print(f"Skipping row with no valid columns: {data_row}")
        return

    # Detect duplicates (rare case if preprocessing fails)
    if len(filtered_row.keys()) != len(set(filtered_row.keys())):
        raise ValueError(f"Duplicate column names detected: {list(filtered_row.keys())}")

    # Check for dropped columns and log them
    dropped = set(data_row.keys()) - valid_columns
    if dropped:
        print(f"Dropping unknown columns: {dropped}")

    placeholders = ', '.join(['%s'] * len(filtered_row))
    column_names = ', '.join(filtered_row.keys())
    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    try:
        session.execute(SimpleStatement(query), list(filtered_row.values()))
    except Exception as e:
        print(f"Failed to insert into Cassandra: {e}")
        print(f"Offending row: {filtered_row}")

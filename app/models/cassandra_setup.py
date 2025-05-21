from cassandra.cluster import Cluster

def get_cassandra_session():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect()
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS etl
    WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
    """)
    session.set_keyspace("etl")
    session.execute("""
    CREATE TABLE IF NOT EXISTS patients (
        pid pid PRIMARY KEY,
        fname text,
        lname text,
        dob date,
        email text,
        gender text,
        cell text,
        city text,
        apartment text,
        street text,
        country text,
        pcode int,
        gestage float,
        
    );
    """)
    return session

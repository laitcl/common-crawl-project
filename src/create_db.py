# This script creates a sqlite3 database to be used in this project
import sqlite3

def create_sqlite_database(filename):
    """ create a database connection to an SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(filename)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def create_links_table(filename):
    """
    Creates a table named links
    """
    conn = sqlite3.connect(filename)
    cursor = conn.cursor()

    query = f"""
        CREATE TABLE IF NOT EXISTS links (
            link_id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_origin VARCHAR(2000),
            link_destination VARCHAR(2000),
            source_archive VARCHAR(500),
            is_valid BOOL
        );
    """
    
    cursor.execute(query)
    

if __name__ == '__main__':
    create_sqlite_database("db/common-crawl-project.db")
    create_links_table("db/common-crawl-project.db")
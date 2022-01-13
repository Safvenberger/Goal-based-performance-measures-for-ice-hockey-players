import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

def create_db_engine(database):
    db_connection_str = 'mysql+pymysql://root:password@localhost/' + database
    engine = create_engine(db_connection_str)
    return engine

def connect_to_db(database):
    try:
        connection = mysql.connector.connect(host='localhost',
                                            database=database,
                                            user='root',
                                            password='password')

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            return connection
        else:
            print("Could not connect to database")

    except Error as e:
        print("Error while connecting to MySQL", e)
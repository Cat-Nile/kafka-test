import psycopg2


def create_db():
    conn = psycopg2.connect(
        database="oslo_city_bike", user='postgres', password='postgres', host='localhost',
        port='5432'
    )

    autocommit = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    conn.set_isolation_level(autocommit)

    cursor = conn.cursor()
    # cursor.execute("DROP DATABASE IF EXISTS Oslo_City_Bike")
    conn.commit()

    # sql = '''CREATE database Oslo_City_Bike'''
    # cursor.execute(sql)
    # conn.commit()

    try:
        cursor.execute("DROP TABLE IF EXISTS Station_Status")
        sql = '''
            CREATE TABLE Station_Status(
                station_id CHAR(20) NOT NULL,
                is_installed int,
                is_renting int,
                is_returning int,
                last_reported int,
                num_bikes_available int,
                num_docks_available int
                )
        '''
        cursor.execute(sql)
    except Exception as e:
        print("Errors occurred: ", e)
    finally:
        conn.commit()
        conn.close()  # close the connection to database to avoid memory leaks


if __name__ == "__main__":
    create_db()

# cursor.execute("DROP TABLE IF EXISTS Station_Status")
# sql = '''CREATE TABLE Station_Status(
#     station_id CHAR(20) NOT NULL,
#     is_installed INT,
#     is_renting INT,
#     is_returning INT,
#     last_reported INT,
#     num_bikes_available INT,
#     num_docks_available INT)'''
#
# cursor.execute(sql)
# conn.commit()
# conn.close()

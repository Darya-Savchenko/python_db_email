import pymysql
from config import host, user, password, db_name
from datetime import datetime, timedelta

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Successfully connected")

    try:
        with connection.cursor() as cursor:
            select_all_dialogs_query = "SELECT * FROM x27_social_dialogs WHERE id_user_from = 12345 OR id_user_to = 12345;"
            cursor.execute(select_all_dialogs_query)
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        with connection.cursor() as cursor:
            new_messages_last_week_query = "SELECT * FROM x27_social_dialogs WHERE (id_user_from = 12345 OR id_user_to = 12345) AND date_created >= %s AND has_new_msg = 1;"
            one_week_ago = datetime.now() - timedelta(weeks=1)
            cursor.execute(new_messages_last_week_query, (one_week_ago,))
            rows = cursor.fetchall()
            for row in rows:
                print(row)

    finally:
        connection.close()

except Exception as ex:
    print("Connection refused...")
    print(ex)

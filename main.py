import os
import json
import pymysql
import smtplib
from dotenv import load_dotenv
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
email_sender = os.getenv('EMAIL_SENDER')
email_password = os.getenv('EMAIL_PASSWORD')
email_receiver = os.getenv('EMAIL_RECEIVER')

select_all_dialogs_query = """SELECT * FROM x27_social_dialogs
                            WHERE id_user_from = 12345 
                            OR id_user_to = 12345;"""
new_messages_last_week_query = """SELECT * FROM x27_social_dialogs 
                                WHERE (id_user_from = 12345 OR id_user_to = 12345) 
                                AND date_created >= %s 
                                AND has_new_msg = 1;"""
email_subject = "Dialogs for 12345 user"


def send_email(subject, sender, receiver, password, message_body):
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = subject

    msg.attach(MIMEText(message_body, 'plain'))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        return "The message was sent"
    except Exception as ex:
        return f"An error occurred: {ex}"


def fetch_data(host, user, password, db_name):
    try:
        connection = pymysql.connect(
            host=host, port=3306, user=user, password=password, database=db_name, cursorclass=pymysql.cursors.DictCursor
        )
        print("Successfully connected")
        try:
            with connection.cursor() as cursor:
                cursor.execute(select_all_dialogs_query)
                all_dialogs = cursor.fetchall()

                one_week_ago = datetime.now() - timedelta(weeks=1)
                cursor.execute(new_messages_last_week_query, (one_week_ago,))
                dialogs_last_week = cursor.fetchall()
        finally:
            connection.close()
        return all_dialogs, dialogs_last_week
    except Exception as ex:
        print("Connection refused or error occurred")
        print(ex)
        return [], []


all_dialogs, dialogs_last_week = fetch_data(db_host, db_user, db_password, db_name)

message = (
    f"All dialogs:\n{json.dumps(all_dialogs, indent=4, default=str)}\n\n"
    f"Dialogs for the last week:\n{json.dumps(dialogs_last_week, indent=4, default=str)}"
)


send_email(email_subject, email_sender, email_receiver, email_password, message)

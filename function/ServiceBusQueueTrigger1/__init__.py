import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info(
        'Python ServiceBus queue trigger processed message: %s', notification_id)

    conn = psycopg2.connect(dbname="techconfdb", user="serali@hellocelloserver",
                            password="udacity@2021", host="hellocelloserver.postgres.database.azure.com")
    print(type(conn))
    cur = conn.cursor()
    print(type(cur))

    try:

        cur.execute(
            f"SELECT message, subject FROM notification WHERE id = {notification_id};")
        notification = cur.fetchall()
        print(notification)

        cur.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cur.fetchall()

        for attendee in attendees:
            subject = f'{attendee[0]}: {notification[0]}'

            print(subject)

            message = Mail(
                from_email="info@techconf.com",
                to_emails=f"{attendee[2]}",
                subject=subject,
                plain_text_content=f"{notification[0]}")

            print(message)

            sg = SendGridAPIClient(
                "")
            print(sg)
            response = sg.send(message)
            print(response.status_code)
            print(response.body)
            print(response.headers)

        notification_completed_date = datetime.utcnow()
        notification_status = f'Notified {len(attendees)} attendees'
        cur.execute(
            f"UPDATE notification SET status = '{notification_status}', completed_date = '{notification_completed_date}' WHERE id = '{notification_id}'")

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        cur.close()
        conn.close()

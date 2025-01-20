"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

TOPIC = 'toll'
DATABASE = 'tolldata'
USERNAME = 'postgres'
PASSWORD = 'mwsNnWQCQJ7U7t7H7dfJRN02'
HOST = '172.21.19.37'  # E.g., 'localhost' or an IP address
PORT = 5432  # Default PostgreSQL port

print("Connecting to the database")
try:
    # Establishing the connection to PostgreSQL
    connection = psycopg2.connect(
        dbname=DATABASE,
        user=USERNAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
except Exception as e:
    print(f"Could not connect to database. Please check credentials. Error: {e}")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from Kafka
    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table
    sql = "INSERT INTO livetolldata (timestamp, vehicle_id, vehicle_type, toll_plaza_id) VALUES (%s, %s, %s, %s)"
    try:
        cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
        connection.commit()
        print(f"A {vehicle_type} was inserted into the database")
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
        connection.rollback()

# Closing the connection
connection.close()

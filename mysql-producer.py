import argparse
import uuid
from uuid import uuid4
# from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List
import mysql.connector
from mysql.connector import Error
import datetime

my_conn = mysql.connector.connect(
    host = 'localhost',
    username = 'abc',
    password = 'password',
    database = 'kafka_db'
    )
my_cursor = my_conn.cursor()

columns=['emp_id','emp_name','salary','created_at']

# Intenationally removed all below secret keys to avoid any security breach.

API_KEY = 'FSKSUURRLODAIUFP'
ENDPOINT_SCHEMA_URL  = 
API_SECRET_KEY = 
BOOTSTRAP_SERVER = <Bootstrap>
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 
SCHEMA_REGISTRY_API_SECRET = 


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Mysql:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record

mysql_l:List[Mysql]=[]
def table_instance():

        query = '''SELECT emp_id,emp_name,salary,unix_timestamp(created_at) as created_at FROM kafka_topic
        where created_at > (SELECT max(created_at) from watermark)
        '''
        my_cursor.execute(query)
        results = my_cursor.fetchall()
        records = print(f"Number of new records: {len(results)}")
        try:
            print("Updating watermark table...")
            update_watermark()
            print("Watermark table updated.")
        except Exception as e:
            print(f"Error updating watermark table: {e}")
        for row in results:
                message = {
                'emp_id': row[0],
                'emp_name': row[1],
                'salary': row[2],
                'created_at': row[3],
                }
           
                cls_message = Mysql(message)
                mysql_l.append(cls_message)
                yield cls_message

def update_watermark():
    query = "SELECT MAX(emp_id), MAX(created_at) FROM kafka_topic"
    my_cursor.execute(query)
    result = my_cursor.fetchone()
    print("Results of SELECT query: ", result)

    insert_query = "INSERT IGNORE INTO watermark (emp_id, created_at) VALUES (%s, %s)" # This query will not insert duplicate records
    my_cursor.execute(insert_query, (result[0], result[1]))
    my_conn.commit()
    print("Watermark table updated.")


def mysql_to_dict(cls_message:Mysql, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return cls_message.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
        # Define the new schema with the added fields
   
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = 'mysql-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, mysql_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(5.0)

        try:    
            
            for cls_message in table_instance():
                print(cls_message)
                message_id = str(uuid.uuid4()) 
                producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=json_serializer(cls_message, SerializationContext(topic, MessageField.VALUE)),
                            headers= [('message_id',message_id)],
                            on_delivery=delivery_report)
            
        except KeyboardInterrupt:
                pass
        except ValueError:
                print("Invalid input, discarding record...")
                pass

        print("\nFlushing records...")
        producer.flush()

main("mysql")
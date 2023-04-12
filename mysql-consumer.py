import argparse
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from time import mktime
from confluent_kafka import TopicPartition


# Intenationally removed all below secret keys to avoid any security breach.

API_KEY =
ENDPOINT_SCHEMA_URL  =
API_SECRET_KEY = 
BOOTSTRAP_SERVER = 
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
   
    @staticmethod
    def dict_to_mysql(data:dict,ctx):
        return Mysql(record=data)
    
    def mysql_to_dict(self):
        self.created_at = datetime.fromtimestamp(self.created_at)

        return {
            'emp_id': self.emp_id,
            'emp_name': self.emp_name,
            'salary': self.salary,
            'created_at': self.created_at }
    def __str__(self):
        return f"{self.record}"
def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = 'mysql-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Mysql.dict_to_mysql)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

#Configure connection to target cassandra database to insert records from the topic

    cloud_config= {
            'secure_connect_bundle': '/config/workspace/secure-connect-cassandra-db.zip'
                    }
    auth_provider = PlainTextAuthProvider('xpoIgEMHZcMSgWiiraBiZtzQ', 'ITgu-4G.20HGM0gpaO5Mk2+X97eafnrnyeJXexW08QCHcI5d+rHKYQDKDf,D9xUpwlRvsy7MzK94XQwFvUaTz7a7jhW,1itUt,xAOdHyvDsIRHN4E,T+iXsiZrZr010B')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    last_committed_offsets = {}
    try:
        while True:

            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)

            if msg is None:
                print("There are no messages in queue yet")
                continue
            print("Reading the messages")
            mysql_v = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if mysql_v is not None:
                    session = cluster.connect()
                    data = mysql_v.mysql_to_dict()
                    print(data)
                    session.execute(f"INSERT INTO mysql_data.kafka_t (emp_id,emp_name, emp_salary, created_at) VALUES ({data['emp_id']},'{data['emp_name']}', {data['salary']}, '{data['created_at'].isoformat()}');")
                    session.shutdown()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
main("mysql")
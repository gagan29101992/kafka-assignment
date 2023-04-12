import mysql.connector
from mysql.connector import Error
import random
import string
import time

mydb = mysql.connector.connect(
    host = 'localhost',
    username = 'abc',
    password = 'password'
    )

#print(mydb)

mycursor = mydb.cursor()
db_name = "kafka_db"
try:
    mycursor.execute(f"create database {db_name}")
    #print(mycursor)
except Error as e:
    print("database creation failed due to ", e)
dblist = []
mycursor.execute(f"use {db_name}")
mycursor.execute("select database()")
for current_db in mycursor:
    print(current_db)

table_name = 'kafka_t'
try:
    create_table = '''
                create table if not exists kafka_t(
                    emp_id INT AUTO_INCREMENT PRIMARY KEY,
                    emp_name varchar(10),
                    salary int,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
'''
    mycursor.execute(create_table)
except Error as e:
    print("Couldn't create table ", e)

insert_into_query = """
                    INSERT INTO kafka_topic
                    (emp_name, salary)
                    VALUES (%s,%s )
                    """
records = []  

for i in range(1,21):
    
    str_value = (''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5)))
    value = random.randint(4000,7000)
    # records.append((str_value,value))
    record = (str_value,value)

    # mycursor.executemany(insert_into_query, records)
    mycursor.execute(insert_into_query,record)
    mydb.commit()
    time.sleep(10)

# mycursor.execute("update kafka_t set emp_name='xxxx' where salary < 400")

# mycursor.execute("select count(*) from kafka_t")
# for row in mycursor:
#     print(row)
    
mycursor.close()





# kafka-assignment

In producer, I have used timestamp column and saved the previous timestamp value in 'watermar' table so that producer doesn't send any records earlier than that to topic. 

Deleted all secret keys etc. to avoid any security issues hence please assume we have put all those API keys and passwords etc.

In consumer class, connection to cassadnra cluster was created and it is dumping all the records to a table in cassandra.

NOTE- I did research and this is not a good way to dump data from database tables to kafka topics instead we should make use of kafka connectors which make uses of CDC tools to capture data changes on database. I understand this assignment was given from learning purpose however in real time industrial projects, for databases we should use connectors instead of making use of SQL queries inside producer.

# Overview

This is an example to demonstrate of how to use DSE 6 Anlytics (Spark) and [Spark-Cassandra connector](https://github.com/datastax/spark-cassandra-connector) for mass deletion of data from a C* table, matching joint conditions from another C* table. 

## Scenario Review

There are 2 C* tables, let's call them **inventory** and **facility** separtely. They have the following primary key structures as below:
- **inventory**: ((facility_id, base_upc), location)
- **facility**: (facility_id) 

For **facility** table, there're 2 non-key columns called **division** and **store** separately, which contain the information of each facility's division and store information.

The goal is to delete all inventory items that belong to a certain divsion and/or store.


# Overview
This is an example to demonstrate of how to use DSE 6 Anlytics (Spark) and [Spark-Cassandra connector](https://github.com/datastax/spark-cassandra-connector) for mass deletion of data from a C* table, matching joint conditions from another C* table. 

## Scenario Review
There are 2 C* tables, let's call them **inventory** and **facility** separately. They have the primary key structures as below:
- **inventory**: ((facility_id, base_upc), location)
- **facility**: (facility_id) 

For **facility** table, there're 2 non-key columns called **division** and **store** separately, which contain the information of each facility's division and store information.

The goal is to delete all inventory items that belong to a certain divsion and/or store.

# Program Overview
The program is develoepd using IntelliJ IDEA CE as a Scala sbt project. The project structure in Intellij is as below:
<p>
  <img src="https://github.com/yabinmeng/invdel_spark/blob/master/src/resources/intellij_project.png" width="300" alt="IntelliJ Project Structure">
</p>

## Program Constants
For simplicity purpose, there are four paramaters that are hard-coded in the program:
- C* keyspace name
- C* facility table name
- C* inventory table name
- DSE spark node IP
```
  val inv_keyspace = "<C*_keyspace_name>"
  val facility_detail_tbl = "<C*_facility_table_name>"
  val inventory_tbl = "<C*_inventory_table_name>"
  val dseSparkHostIp = "<DSE_Spark_Node_IP>"
```  

Further improvement can be made to read these values from either as command line parameters or from a property file.

## Program Execution
The program is intended to run as a Spark job that will be submitted to a DSE Anlytics cluster for execution. Data stored in C* table is read/updated/delted in Spark by utilizing Spark-Cassandra connector APIs.

The command to submit the job is something like below:
```
dse spark-submit --class com.example.InventoryCleanup invdel_spark-assembly-1.0.jar --sto <store_name> --div <divsion_name>
```

The program accepts 2 input parameters:
- **--sto <store_name>**: specific store name of which the inventory to be deleted
- **--div <division_name>**: specific division name of which the inventory to be deleted

You can specify both parameters at the same time, but not none!


## Build a Uber Jar
In order to submit the program properly, a Uber jar is needed. This is achieved through "sbt-assembly" plugin as described in **assembly.sbt** file, as below:
```
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
```

Then from command line, at the home folder of the project, run the following command:
```
sbt assembly
```

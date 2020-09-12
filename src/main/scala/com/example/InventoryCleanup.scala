package com.example

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._


object InventoryCleanup extends App {

  val inv_keyspace = "<C*_keyspace_name>"
  val facility_detail_tbl = "<C*_facility_table_name>"
  val inventory_tbl = "<C*_inventory_table_name>"

  val usage = """
    Usage: InventoryCleanup --nodeip "<dse_analytics_node_ip>" [--sto "<store_name>"] [--div "<division_name>"]
  """

  if ( (args.length != 4) && (args.length != 6) ) {
    println("\nIncorrect input parameter numbers!")
    println(usage)
    System.exit(10)
  }

  def isEmptyStr(x: String) = Option(x).forall(_.isEmpty)

  var dseSparkHostIp = ""
  var store_name = ""
  var division_name = ""

  args.sliding(2, 2).toList.collect {
    case Array("--nodeip", sparkNodeIp: String) => dseSparkHostIp = sparkNodeIp
    case Array("--sto", argStore: String) => store_name = argStore
    case Array("--div", argDivision: String) => division_name = argDivision
  }

  if ( isEmptyStr(dseSparkHostIp) || (isEmptyStr(store_name) && isEmptyStr(division_name)) ) {
    println("\nIncorrect input parameter values!")
    print(usage)
    System.exit(20)
  }

  //=== Debug purpose ===
  //println("dseSparkHostIp = " + dseSparkHostIp)
  //println("store_name = " + store_name)
  //println("division_name = " + division_name)

  case class FacilityDetail
  (
    facility_id: String,
    division: String,
    store: String
  )

  val spark = SparkSession
    .builder()
    .master("dse://" + dseSparkHostIp + ":9042")
    .appName("InventoryCleanup")
    .config("spark.cassandra.connection.host", dseSparkHostIp)
    .getOrCreate()

  import spark.implicits._

  // read facility info, by the store and division
  //   info that are provided as input parameters
  var facilityDetailDF = spark
      .read
      .cassandraFormat(facility_detail_tbl, inv_keyspace)
      .options(ReadConf.SplitSizeInMBParam.option(32))
      .load()
      .select("facility_id", "division", "store")
      .as[FacilityDetail]

  if (!isEmptyStr(store_name)) {
    facilityDetailDF = facilityDetailDF.where("store = '" + store_name + "'")
  }

  if (!isEmptyStr(division_name)) {
    facilityDetailDF = facilityDetailDF.where("division = '" + division_name + "'")
  }

  facilityDetailDF.cache()

  val facilityList = facilityDetailDF
    .map(fd => fd.facility_id)
      .collect()
      .toList

  //== Debug purpose ==
  //facilityDetailDF.printSchema()
  //facilityDetailDF.show()
  //println(facilityList)



  // read inventory table with all facilities that match
  //   the selected list from above
  var inventoryDF = spark
    .read
    .cassandraFormat(inventory_tbl, inv_keyspace)
    .options(ReadConf.SplitSizeInMBParam.option(32))
    .load()
    .select("facility_id", "base_upc", "location")
    .filter($"facility_id".isin(facilityList:_*))


  // == Debug purpose ==
  //inventoryDF.show()

  // Delete the inventories satisfying the store/division conditions
  inventoryDF.rdd.deleteFromCassandra(inv_keyspace, inventory_tbl)
  println("Inventory deletion for store/division (" + store_name + "/" + division_name + ") completes!" )

  spark.close()
  System.exit(0)
}

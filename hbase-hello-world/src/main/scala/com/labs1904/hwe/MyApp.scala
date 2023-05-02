package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Get, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}
import scala.collection.JavaConverters._

object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", System.getenv("HBASE"))
      connection = ConnectionFactory.createConnection(conf)
      // Example code... change me
      val table = connection.getTable(TableName.valueOf(Bytes.toBytes("bswyers:users")))
      //Challenge 1
      val get = new Get(Bytes.toBytes("10000001")).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
      val result = table.get(get)
      logger.debug(Bytes.toString(result.value()))
      //Challenge 2
      val put = new Put(Bytes.toBytes("99"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("username"), Bytes.toBytes("DE-HWE"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("The Panther"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("F"))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("favorite_color"), Bytes.toBytes("pink"))
      val putResult = table.put(put)
      val pGet = new Get(Bytes.toBytes("99"))
      val pGetResult = table.get(pGet)
      logger.debug(pGetResult)
      //Challenge 3
      val myScan = new Scan().withStartRow(Bytes.toBytes("10000001")).withStopRow(Bytes.toBytes("10006001"))
      val scanResult = table.getScanner(myScan)
      val scanList = scanResult.iterator()
      var count = 0;
      scanList.forEachRemaining(x => count = count + 1)
      logger.debug(count)
      //Challenge 4
      val delete = new Delete(Bytes.toBytes("99"))
      table.delete(delete);
      val delResult = table.get(pGet);
      logger.debug(delResult)
      //Challenge 5
      //val get = new Get(Bytes.toBytes("10000001")).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
      val keys = List("9005729", "500600", "30059640", "6005263", "800182")
      val gets: List[Get]  = keys.map(id => new Get(Bytes.toBytes(id)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
      val javaGets = gets.asJava
      val getsResult = table.get(javaGets)
      getsResult.foreach(x => logger.debug(Bytes.toString(x.value())))
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}

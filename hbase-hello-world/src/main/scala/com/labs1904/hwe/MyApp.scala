package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}
import com.labs1904.hwe.util.HBaseConnection._


object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
      connection = ConnectionFactory.createConnection(conf)
      // Example code... change me
      val table = connection.getTable(TableName.valueOf(HBASE_TABLE))
      val get = new Get(Bytes.toBytes("row-key"))
      val result = table.get(get)
      logger.debug(result)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}

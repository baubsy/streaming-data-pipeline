package com.labs1904.hwe
import com.labs1904.hwe.util.HBaseConnection._

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

object ConnectionTest {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)


  def main(args: Array[String]) = {

    var connection: Connection = null
    try {
      logger.debug("Starting app")
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
      connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("hwe:connection_test"))
      val get = new Get(Bytes.toBytes("rowkey"))
      val result = table.get(get)
      val message = Bytes.toString(result.getFamilyMap(Bytes.toBytes("f1")).get(Bytes.toBytes("test")))
      logger.debug(message)

    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }


  }
}

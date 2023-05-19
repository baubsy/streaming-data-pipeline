package com.labs1904.hwe.util

/*
The connection class requires creating 1 environment variable before running the app and possibly before starting IntelliJ (see further explanation below).  Please fill the following environment variables with values provided by the HWE admins:

- HWE_HBASE_ZOOKEEPER_QUORUM
- HWE_HBASE_TABLE

If the environment variables are not present, an exception will be thrown informing developers
what environment variable(s) are missing.

Note that some environments like Windows need to have IntelliJ restarted before the environment variables will be present in the environment (I haven't tested Linux and Mac).
 */

object HBaseConnection {
  private def getConnectionValue(envVarName: String): String = {
    val envVar: Option[String] = sys.env.get(envVarName)
    if (envVar.isEmpty) {
      // Note that if you get this even after creating the environment variable you might need
      // to restart IntelliJ to get the environment variable to be loaded into the process.
      throw new IllegalStateException(s"Environment variable named '$envVarName'" +
        s" was not found. Please create the environment variable named '$envVarName' with" +
        " the values given by the HWE admins.\r\n" +
        "Note you may need to restart the IDE after setting the environment variable.");
    }
    envVar.get
  }

  /** Connection string to the HBase database.
   * This is provided by the HWE admins.
   *
   * @throws IllegalStateException if the environment variable is not found
   */
  @throws(classOf[IllegalStateException])
  val HBASE_ZOOKEEPER_QUORUM: String = {
    getConnectionValue("HWE_HBASE_ZOOKEEPER_QUORUM")
  }

  /** Your specific HBase table used in lab.  Value should be:
   * `<first letter of first name><last name>:users`.
   * For example, Kit's table will be `kmenke:users`.
   *
   * @throws IllegalStateException if the environment variable is not found
   */
  @throws(classOf[IllegalStateException])
  val HBASE_TABLE: String = {
    getConnectionValue("HWE_HBASE_TABLE")
  }

}

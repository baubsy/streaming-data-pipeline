# HBase Hello World

An example maven project for reading and writing to HBase using Scala.

To open the project in Intellij, go to Open and navigate to the pom.xml in the root of this directory. When prompted,
choose to open as a project. 

Navigate to MyApp.scala to start editing!

# Lab Setup

Before starting the lab please set the following environment variables:
 - `HWE_HBASE_ZOOKEEPER_QUORUM` - this value allows the apps to connect to the HBase database.  This is provided by the HWE admins.
 - `HWE_HBASE_TABLE` - this value is a table specific to each user. The format of it will be `<first letter of first name><last name>:users`.  For example, Kit's table will be `kmenke:users`.

Note that some environments like Windows need to have IntelliJ restarted before the environment variables will be present in the environment (I haven't tested Linux and Mac).

# The Lab

Challenge #1: What is user=10000001 email address? Determine this by using a Get that *only* returns that user's email 
address, not their complete column list.

Challenge #2: Write a new user to your table with:

 - Rowkey: 99
 - username: DE-HWE
 - name: The Panther
 - sex: F
 - favorite_color: pink

(Note that favorite_color is a new column qualifier in this table, and we are not specifying some other columns every 
other record has: DOB, email address, etc.)

Challenge #3: How many user IDs exist between 10000001 and 10006001? (Not all user IDs exist, so the answer is *not* 
6000)

Challenge #4: Delete the user with ID = 99 that we inserted earlier.

Challenge #5: There is also an operation that returns multiple results in a single HBase "Get" operation. Write a single
HBase call that returns the email addresses for the following 5 users: 9005729, 500600, 30059640, 6005263, 800182 (Hint: 
Look at the Javadoc for "Table")

# Tips

## Useful resources:

https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Get.html
https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Put.html
https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Scan.html
https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Delete.html
https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Table.html
https://hbase.apache.org/2.1/apidocs/org/apache/hadoop/hbase/client/Result.html

## Useful functions:

Converting a string to a byte array: `Bytes.toBytes("mystring")`

Converting a byte array to a string: `Bytes.toString(byteArray)`

Converting a Scala list to a Java list:
```
import scala.collection.JavaConverters._
val scalaList = List("a", "b", "c")
val javaList = scalaList.asJava
```

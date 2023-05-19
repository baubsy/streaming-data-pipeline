package com.labs1904.hwe.util

import java.nio.file.Paths

object Constants {
  // TODO: this is configured to use kafka running locally, change it to your cluster
  val BOOTSTRAP_SERVER : String = "CHANGEME"
  val DEFAULT_TOPIC: String = "connection-test"
  val USERNAME: String = "CHANGEME"
  val PASSWORD: String = "CHANGEME"
  //Use this for Windows
  val TRUST_STORE: String = {
    val url = getClass.getResource("/kafka.client.truststore.jks")
    if (url == null) throw new Exception("Unable to find kafka.client.truststore.jks in resources, is the project opened correctly?")
    else Paths.get(url.toURI()).toString
  }
}

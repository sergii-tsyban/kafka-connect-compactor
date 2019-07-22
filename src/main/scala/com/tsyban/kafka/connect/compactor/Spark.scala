package com.tsyban.kafka.connect.compactor

import org.apache.spark.sql.SparkSession

object Spark {

  def sparkSession(name: String, master: Option[String] = None): SparkSession = {
    val builder = master match {
      case Some(masterName) => SparkSession.builder().master(masterName)
      case None => SparkSession.builder()
    }
    builder
      .appName(name)
      .getOrCreate()
  }

}

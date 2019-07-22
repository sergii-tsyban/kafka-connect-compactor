package com.tsyban.kafka.connect.compactor

import java.util

import com.typesafe.config.{Config, ConfigFactory}

case class JobConfig(jobName: String, dataSetDir: String, blacklist: List[String], keepSnapshots: Int)

object JobConfig {

  def apply(config: Config): JobConfig = {
    val jobName: String = config.getString("job.name")
    val pathPrefix: String = config.getString("compaction.dataSetDir")
    val blacklist: util.List[String] = config.getStringList("compaction.blacklist")
    val keepSnapshots: Int = config.getInt("compaction.keepSnapshots")
    new JobConfig(jobName, pathPrefix, blacklist, keepSnapshots)
  }

  def apply(): JobConfig = JobConfig(ConfigFactory.load())
}

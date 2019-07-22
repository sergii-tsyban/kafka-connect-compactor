package com.tsyban.kafka.connect.compactor

import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object Application extends App {

  val logger = Logger.getLogger(Application.getClass)

  logger.info("Compactor job started")

  val hdfsAccess = HDFSAccess(FileSystem.get(new Configuration()))

  val inputArgs = InputArgs(args:_*)
  logger.info(inputArgs)

  val config = inputArgs.configPath match {
    case Some(path) => JobConfig(ConfigFactory.parseReader(hdfsAccess.fileToReader(path)))
    case None => JobConfig()
  }
  logger.info(config)

  val dataSetsToProcess = inputArgs.dataSet.filterNot(config.blacklist.contains(_))
  logger.info(s"For date ${inputArgs.processingDate}, datasets to be compacted: $dataSetsToProcess")

  val dataSetsDirs = dataSetsToProcess.map(dataSetName => s"${config.dataSetDir}/$dataSetName/data/all/")
  val snapshots = dataSetsDirs.map(dir => hdfsAccess.rollingSnapshotOf(dir, config.keepSnapshots))
  snapshots.foreach(path => logger.info(s"Snapshot created ${path.toString}"))

  val partitionDir = inputArgs.processingDate.format(DateTimeFormatter.ofPattern("'p_date'=YYYYMMdd"))
  val targetDirs = dataSetsDirs.map(dataSetDir => s"$dataSetDir/$partitionDir")
  val allFilesToCompact = hdfsAccess.collectFilesRecur(targetDirs)
  logger.info(s"Files collected for compaction:\n${allFilesToCompact.map(_.getName + "\n").sorted}")

  val units = PartitionBasedCompaction().toCompactionUnits(allFilesToCompact)
  logger.info(s"Compaction Units:\n$units")

  val spark: SparkSession = Spark.sparkSession(config.jobName)

  SparkCompactor(spark, hdfsAccess).compact(units)

  logger.info("Compactor job finished successfully")

}

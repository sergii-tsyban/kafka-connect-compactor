package com.tsyban.kafka.connect.compactor

import com.tsyban.kafka.connect.compactor.SparkCompactor.CountMismatchException
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Compactor {

  def compact(units: List[CompactionUnit])
}

object SparkCompactor {

  case class CountMismatchException(msg: String) extends RuntimeException(msg)

  def apply(spark: SparkSession, hdfsAccess: HDFSAccess): SparkCompactor = new SparkCompactor(spark, hdfsAccess)
}

class SparkCompactor(spark: SparkSession, hdfsAccess: HDFSAccess) extends Compactor {

  val logger = Logger.getLogger(SparkCompactor.getClass)

  override def compact(units: List[CompactionUnit]): Unit = {
    units.foreach(compact)
  }

  private def compact(unit: CompactionUnit): Unit = {
    val filesToCompact = unit.filesToCompact.map(_.toString)
    val filesParentDir = unit.filesToCompact.head.getParent.toString
    val compactOutputDir = s"$filesParentDir/compacted/${unit.compactedFileName}"
    val fileFormat = inferFormat(filesToCompact.head)

    logger.info(s"Processing: $unit")

    val df = read(filesToCompact, fileFormat)
    val countBefore = df.count()
    val countAfter = writeCompacted(df, compactOutputDir, fileFormat)

    if (countBefore == countAfter) {
      hdfsAccess.removeFiles(unit.filesToCompact)
      hdfsAccess.renameFileByPattern(s"$compactOutputDir/part-*", s"$filesParentDir/${unit.compactedFileName}")
      hdfsAccess.removeDir(s"$filesParentDir/compacted")
    } else {
      hdfsAccess.removeDir(s"$filesParentDir/compacted")
      throw CountMismatchException(s"Count mismatch, before compaction: $countBefore, after compaction: $countAfter")
    }
  }

  private def read(filesToCompact: List[String], format: String) = {
    spark
      .read
      .format(format)
      .load(filesToCompact: _*)
  }

  private def writeCompacted(df: DataFrame, outDir: String, format: String) = {
    df.coalesce(1)
      .write
      .format(format)
      .save(outDir)
    read(List(outDir), format).count()
  }

  private def inferFormat(name: String): String = name.takeRight(name.length - name.lastIndexOf('.') - 1) match {
    case "avro" => "com.databricks.spark.avro"
    case "txt" => "text"
    case format => format
  }
}

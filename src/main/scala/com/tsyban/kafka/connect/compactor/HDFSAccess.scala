package com.tsyban.kafka.connect.compactor

import java.io.{InputStream, InputStreamReader, Reader}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSAccess {

  def apply(hadoopFS: FileSystem): HDFSAccess = new HDFSAccess(hadoopFS)
}

class HDFSAccess(val hadoopFS: FileSystem) {

  val snapShotNameTemplate: DateTimeFormatter = DateTimeFormatter.ofPattern("'.s_'yyyy_MM_ddTHHmmss")

  def fileToReader(filePath: String): Reader = {
    new InputStreamReader(hadoopFS.open(new Path(filePath)))
  }

  def removeDir(dirPath: String): Unit = {
    hadoopFS.delete(new Path(dirPath), true)
  }

  def removeFiles(filesPaths: List[Path]): Unit = {
    filesPaths.foreach(hadoopFS.delete(_, false))
  }

  def collectFilesRecur(dirPaths: List[String]): List[Path] = {
    dirPaths
      .flatMap(path => hadoopFS.listFiles(new Path(path), true))
      .filter(_.isFile)
      .map(_.getPath)
  }

  def renameFileByPattern(pattern: String, targetName: String): Boolean = {
    val renameFrom = hadoopFS.globStatus(new Path(pattern)).head.getPath
    hadoopFS.rename(renameFrom, new Path(targetName))
  }

  def rollingSnapshotOf(dirPath: String, maxCount: Int): Option[Path] = maxCount match {
    case n if n <= 0 => None
    case n =>
      val prevSnapshots = hadoopFS.globStatus(new Path(s"$dirPath/.s")).map(_.getPath).sortBy(_.getName).reverse
      prevSnapshots.splitAt(n - 1)._2.foreach(hadoopFS.delete(_, true))
      val snapshot = hadoopFS.createSnapshot(new Path(dirPath), snapShotNameTemplate.format(LocalDate.now()))
      Some(snapshot)
  }

}

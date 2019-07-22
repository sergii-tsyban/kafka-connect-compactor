package com.tsyban.kafka.connect.compactor

import com.tsyban.kafka.connect.compactor.PartitionBasedCompaction.KCFile
import org.apache.hadoop.fs.Path

import scala.annotation.tailrec
import scala.util.matching.Regex

case class CompactionUnit(filesToCompact: List[Path], compactedFileName: String)

trait CompactionStrategy {

  def toCompactionUnits(files: List[Path]): List[CompactionUnit]
}

object PartitionBasedCompaction {

  case class KCFile(path: Path, topicName: String, partition: String, startOffset: String, endOffset: String, extension: String) extends Ordered[KCFile] {
    override def compare(that: KCFile): Int = this.path.getName.compare(that.path.getName)
  }

  object KCFile {

    val kcFileNameRegex: Regex = "^(.+)\\+(\\d{1,})\\+(\\d{3,})\\+(\\d{3,})\\.(.+)$".r

    def apply(path: Path): Option[KCFile] = path.getName match {
      case kcFileNameRegex(topicName, partition, startOffset, endOffset, extension) =>
        Some(KCFile(path, topicName, partition, startOffset, endOffset, extension))
    }

  }

  def apply(): PartitionBasedCompaction = new PartitionBasedCompaction()
}

class PartitionBasedCompaction extends CompactionStrategy {

  def toCompactionUnits(files: List[Path]): List[CompactionUnit] = {

    def gapBetween(current: KCFile, next: KCFile) = next.startOffset.toLong - current.endOffset.toLong > 1

    def mergedNameFor(groupedFiles: List[KCFile]) = {
      val first = groupedFiles.head
      val last = groupedFiles.last
      s"${first.topicName}+${first.partition}+${first.startOffset}+${last.endOffset}.${last.extension}"
    }

    @tailrec
    def subgroupByOffsetsGaps(files: List[KCFile], partAcc: List[KCFile] = List(), resAcc: List[List[KCFile]] = List()): List[List[KCFile]] = {
      val (head, tail) = files.splitAt(2)
      (head.head, head.last) match {
        case (current, next) if current == next => (current :: partAcc) :: resAcc
        case (current, next) if gapBetween(current, next) => subgroupByOffsetsGaps(next :: tail, List(), (current :: partAcc) :: resAcc)
        case (current, next) => subgroupByOffsetsGaps(next :: tail, current :: partAcc, resAcc)
      }
    }

    def asCompactionUnit(files: List[Path]) = {
      files
        .flatMap(KCFile(_))
        .groupBy(kcFile => (kcFile.topicName, kcFile.partition))
        .filter(_._2.size > 1)
        .map(group => subgroupByOffsetsGaps(group._2.sorted))
        .reduce(_ ::: _)
        .map{compactionGroup =>
          val sorted = compactionGroup.sorted
          CompactionUnit(sorted.map(_.path), mergedNameFor(sorted))
        }
    }

    asCompactionUnit(files)
  }
}

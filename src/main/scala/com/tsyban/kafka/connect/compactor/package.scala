package com.tsyban.kafka.connect

package object compactor {

  import scala.collection.JavaConverters._

  implicit def javaListToScala[T](from: java.util.List[T]): scala.List[T] = from.asScala.toList
  implicit def scalaListToJava[T](from: scala.List[T]): java.util.List[T] = from.asJava
  implicit def javaIterToScala[T](from: java.util.Iterator[T]): scala.Iterator[T] = from.asScala
  implicit def remoteIterator2ScalaIterator[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) : scala.collection.Iterator[T] = ScalaRemoteIterator[T](underlying)

  case class ScalaRemoteIterator[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) extends scala.collection.AbstractIterator[T] with scala.collection.Iterator[T] {
    def hasNext: Boolean = underlying.hasNext
    def next(): T = underlying.next()
  }

}

package com.tsyban.kafka.connect.compactor

import org.apache.hadoop.fs.Path

class PartitionBasedCompactionSpec extends UnitSpec {

  "PartitionBasedCompaction" should "create three compaction units" in {

    val partitionZero = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017459+0000017461.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017462+0000017467.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017468+0000017484.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017485+0000017487.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017488+0000017490.parquet"
    ).map(new Path(_))

    val partitionOne = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017479+0000017479.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017480+0000017488.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017489+0000017495.parquet"
    ).map(new Path(_))

    val partitionTwo = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+2+0000017595+0000017596.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+2+0000017597+0000017597.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+2+0000017598+0000017603.parquet"
    ).map(new Path(_))

    val filesToCompact = partitionZero ::: partitionOne ::: partitionTwo

    val expectedCompactionUnits = List(
      CompactionUnit(partitionZero, "dataset-one.env-one.public+0+0000017459+0000017490.parquet"),
      CompactionUnit(partitionOne,  "dataset-one.env-one.public+1+0000017479+0000017495.parquet"),
      CompactionUnit(partitionTwo,  "dataset-one.env-one.public+2+0000017595+0000017603.parquet")
    )

    val actualCompactionUnits = PartitionBasedCompaction().toCompactionUnits(filesToCompact)

    assert(actualCompactionUnits.sortBy(_.compactedFileName) == expectedCompactionUnits.sortBy(_.compactedFileName))
  }

  "PartitionBasedCompaction" should "create two compaction units because of broken sequence" in {

    val partitionZero1 = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017459+0000017461.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017462+0000017467.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017468+0000017484.parquet"
    ).map(new Path(_))
    // broken partition sequence
    val partitionZero2 = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000324561+0000325889.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000325890+0000325895.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000325896+0000325900.parquet"
    ).map(new Path(_))

    val filesToCompact = partitionZero1 ::: partitionZero2

    val expectedCompactionUnits = List(
      CompactionUnit(partitionZero1, "dataset-one.env-one.public+0+0000017459+0000017484.parquet"),
      CompactionUnit(partitionZero2, "dataset-one.env-one.public+0+0000324561+0000325900.parquet")
    )

    val actualCompactionUnits = PartitionBasedCompaction().toCompactionUnits(filesToCompact)

    assert(actualCompactionUnits.sortBy(_.compactedFileName) == expectedCompactionUnits.sortBy(_.compactedFileName))
  }


  "PartitionBasedCompaction" should "create one compaction unit, skip single file partition" in {

    val partitionZero = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017479+0000017479.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017480+0000017488.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017489+0000017495.parquet"
    ).map(new Path(_))

    // single files should be ignored - no compaction needed
    val partitionOne = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000000021+0000000025.parquet"
    ).map(new Path(_))

    val filesToCompact = partitionZero ::: partitionOne

    val expectedCompactionUnits = List(
      CompactionUnit(partitionZero, "dataset-one.env-one.public+0+0000017479+0000017495.parquet")
    )

    val actualCompactionUnits = PartitionBasedCompaction().toCompactionUnits(filesToCompact)

    assert(actualCompactionUnits == expectedCompactionUnits)
  }

  "PartitionBasedCompaction" should "create compaction units for different environments" in {

    val partitionZero = List(
     "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017459+0000017461.parquet",
     "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017462+0000017467.parquet",
     "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017468+0000017484.parquet",
     "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017485+0000017487.parquet",
     "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017488+0000017490.parquet",
      //broken sequence
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000027490+0000027591.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000027592+0000027799.parquet"
    ).map(new Path(_))

    val partitionOne = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017479+0000017479.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017480+0000017488.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+1+0000017489+0000017495.parquet"
    ).map(new Path(_))

    val partitionZeroDifferentEnv = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=some-env/dataset-one.some-env.public+0+0000000000+0000000100.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=some-env/dataset-one.some-env.public+0+0000000101+0000000200.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=some-env/dataset-one.some-env.public+0+0000000201+0000000300.parquet"
    ).map(new Path(_))

    val filesToCompact = partitionZero ::: partitionOne ::: partitionZeroDifferentEnv

    val expectedCompactionUnits = List(
      CompactionUnit(partitionZero.take(5), "dataset-one.env-one.public+0+0000017459+0000017490.parquet"),
      CompactionUnit(partitionZero.takeRight(2), "dataset-one.env-one.public+0+0000027490+0000027799.parquet"),
      CompactionUnit(partitionOne,  "dataset-one.env-one.public+1+0000017479+0000017495.parquet"),
      CompactionUnit(partitionZeroDifferentEnv,  "dataset-one.some-env.public+0+0000000000+0000000300.parquet")
    )

    val actualCompactionUnits = new PartitionBasedCompaction().toCompactionUnits(filesToCompact)

    assert(actualCompactionUnits.sortBy(_.compactedFileName) == expectedCompactionUnits.sortBy(_.compactedFileName))
  }

  "PartitionBasedCompaction" should "create one compaction unit, broken order" in {

    val partitionZero = List(
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017459+0000017461.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017462+0000017467.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017468+0000017484.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017485+0000017487.parquet",
      "hdfs://dev-node/data/lake/dataset-one/data/all/date=20190617/env=env-one/dataset-one.env-one.public+0+0000017488+0000017490.parquet"
    ).reverse.map(new Path(_))

    val filesToCompact = partitionZero

    val expectedCompactionUnits = List(
      CompactionUnit(partitionZero.sortBy(_.getName), "dataset-one.env-one.public+0+0000017459+0000017490.parquet")
    )

    val actualCompactionUnits = new PartitionBasedCompaction().toCompactionUnits(filesToCompact)

    assert(actualCompactionUnits == expectedCompactionUnits)
  }
}

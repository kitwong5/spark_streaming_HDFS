package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.util.regex.Pattern
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration

import java.io.OutputStreamWriter


object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: WordCount <inputDirectory> <outputDirectory>")
      System.exit(1)
    }

    // set streaming context with as many working threads as logical cores on your machine
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    // set the batch interval of 3 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // set checkpoint directory
    ssc.checkpoint(".")


    // input variable - set output directory
    val outputDir = args(1)
    // input variable - the textFileStream on the directory
    val lines = ssc.textFileStream(args(0))

    // regex of alphabets only
    val alphaRegex = Pattern.compile("^[a-zA-Z]+$")

    // flattening the input into an array
    val words = lines.flatMap(_.trim().split(" "))
      .filter(word => alphaRegex.matcher(word).matches) //filter only take alphabets words
      .filter(word => word.length >= 3)  //filter out the short words < 3 characters

    // TaskA: Count word frequency and perform reduce
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    // output word count to hdfs
    var runNumA = 0
    wordCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        runNumA += 1
        // set unique directory for TaskA and each run
        val outputPathA = new Path(s"$outputDir/taskA-${f"%%03d".format(runNumA)}")
        // output result to HDFS
        outputToHDFS(rdd, outputPathA)
      }
    }


    // TaskB: Count word pair frequency
    val wordPair = lines.flatMap(line => {
        val words = line.trim.split(" ")
          .filter(word => alphaRegex.matcher(word).matches) //filter only take alphabets words
          .filter(word => word.length >= 3)  //filter out the short words < 3 characters
        // pair words
        for {
          (word1, index1) <- words.zipWithIndex
          (word2, index2) <- words.zipWithIndex
          if index1 != index2  // filter out same word pair
        } yield (word1, word2)
      })
    // count word pair frequency and perform reduce
    val wordPairCounts = wordPair.map(word => (word, 1)).reduceByKey(_ + _)
    // output pair word count to hdfs
    var runNumB = 0
    wordPairCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        runNumB += 1
        // set unique directory for TaskB and each run
        val outputPathB = new Path(s"$outputDir/taskB-${f"%%03d".format(runNumB)}")
        // output result to HDFS
        outputToHDFS(rdd, outputPathB)
      }
    }

    // TaskC: continuously update the count word pair frequency
    // define the update function
    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int]={
      val newCount = runningCount.getOrElse(0) + newValues.sum // the previous + new count
      Some(newCount)
    }
    // count pair word frequency and perform reduce
    val wordPairCountsTaskC = wordPair.map(word => (word, 1))
    val runningCounts = wordPairCountsTaskC.updateStateByKey[Int](updateFunction _)
    // output pair word count to hdfs
    var runNumC = 0
    runningCounts.foreachRDD { rddC =>
      if (!rddC.isEmpty()) {
        runNumC += 1
        // set unique directory for TaskC and each run
        val outputPathC = new Path(s"$outputDir/taskC-${f"%%03d".format(runNumC)}")
        // output result to HDFS
        outputToHDFS(rddC, outputPathC)
      }
    }

    // start streaming
    ssc.start()
    ssc.awaitTermination()
  }

  def outputToHDFS[T](rdd: RDD[T], outputPath: Path): Unit = {
    // map and collect partition index and records from each partition
    val recordsByPartition = rdd.mapPartitionsWithIndex { (partitionIndex, partitionOfRecords) =>
      partitionOfRecords.map(record => (partitionIndex, record))
    }.collect()

    var fileSeq = 0
    val connection = new Configuration()
    val hdfs = FileSystem.get(connection)

    recordsByPartition.foreach { case (partitionIndex, record) =>
      var outputFile = new Path(outputPath, f"part-${partitionIndex}%03d-${fileSeq}.txt")

      // set unique filename
      while (hdfs.exists(outputFile)) {
        fileSeq += 1
        outputFile = new Path(outputPath, f"part-${partitionIndex}%03d-${fileSeq}.txt")
      }

      // output to file
      val stream = hdfs.create(outputFile)
      val writer = new OutputStreamWriter(stream, "UTF-8")
      writer.write(record.toString + "\n")
      writer.close()
      stream.close()
    }
    hdfs.close()
  }

}

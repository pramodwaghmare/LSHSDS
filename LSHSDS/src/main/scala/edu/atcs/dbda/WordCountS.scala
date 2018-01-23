package edu.atcs.dbda

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
 
 
/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 */
object WordCountS {
  def main(args: Array[String]) {
    if (args.length > 0) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
 
    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
 
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("/home/s/git/LSHSDS/LSHSDS/Words")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    println("---------------gggggggggggg--------")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
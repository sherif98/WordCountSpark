package edu.distributed.lab.five

import org.apache.spark.{SparkConf, SparkContext}

/** Implementation of words count in
  * spark.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
//    val inputFile = getClass.getClassLoader.getResource("document.txt").getPath
//    val outputFile = "output"
    
    val inputFile = "hdfs://localhost:9000/PATH_TO_INPUT_FILE_ON_HDFS"
    val outputFile = "hdfs://localhost:9000/PATH_TO_INPUT_FILE_ON_HDFS"
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = for {
      line <- input
      word <- line.split(" ")
    } yield (word, 1)
    words.reduceByKey(_ + _).saveAsTextFile(outputFile)
  }
}

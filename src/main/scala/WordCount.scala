package org.dezyre

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args:Array[String])={

    //creating a data collection
    val data = List("First line of the collection",
    "Second line which holds demo data",
    "Third line to make it collection larger",
    "Fourth line to expand the collection")

    //creating a spark session
    val spark = SparkSession.
      builder().
      appName("Testing Word Count").
      master("local").getOrCreate();

    //creating a RDD: resilient distributed Dataset
    val dataRdd = spark.sparkContext.parallelize(data)

    //calculating wordcount
   val wordCount =  dataRdd.flatMap(line => line.split(" ")).
      map(words => (words,1)).
      reduceByKey((sum,count) => sum+count).collect()

    wordCount.foreach(println)
  }
}

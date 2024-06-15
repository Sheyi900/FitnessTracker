package org.dezyre

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object FitnessTrackerDatajob {
  def main(args: Array[String]): Unit = {
    println("Analytics of Fitness Tracker Data")

    val sparkSession: SparkSession = getSparkSession()
    val rawFitnessData: Dataset[Row] = scanRawFitnessData(sparkSession)

    // Uncomment for debugging or schema validation
    // rawFitnessData.printSchema()
    // rawFitnessData.show(false)

    val formattedFitnessData: Dataset[Row] = formatData(rawFitnessData)
    formattedFitnessData.cache()

    val highestToLowestUsersCalorieBurnt: Dataset[Row] =
      caloriesBurntBestToWorst(formattedFitnessData)

    highestToLowestUsersCalorieBurnt.show(false)

    val femalesBestToWorstActivity: Dataset[Row] =
      activityUsedBestToWorstAmongFemales(formattedFitnessData)

    femalesBestToWorstActivity.show(false)

    val femalesBestToWorstActivitySQL: Dataset[Row] =
      activityUsedBestToWorstAmongFemalesSQLStyle(sparkSession, formattedFitnessData)

    femalesBestToWorstActivitySQL.show(false)

    // For testing purposes, sleep to keep Spark session alive
    // Thread.sleep(1000000)
    sparkSession.stop()
  }

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Fitness Tracker Data Job")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .master("local[*]") // Change to appropriate master URL for production
      .getOrCreate()
  }

  def scanRawFitnessData(sparkSession: SparkSession): Dataset[Row] = {
    sparkSession
      .read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(System.getenv("path") + "Fitness_tracker_data.csv")
  }

  def formatData(dataset: Dataset[Row]): Dataset[Row] = {
    formatActivityColumn(formatTheTimeStampColumn(dataset))
  }

  def formatActivityColumn(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.withColumnRenamed("activity", "activity_raw")
      .withColumn("activity", regexp_replace(col("activity_raw"), "_", ""))
      .select(dataset.columns.head, dataset.columns.tail: _*)
  }

  def formatTheTimeStampColumn(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.withColumnRenamed("time_stamp", "time_stamp_raw")
      .withColumn("time_stamp", to_timestamp(col("time_stamp_raw"), "dd:MM:yy HH:mm"))
      .select(dataset.columns.head, dataset.columns.tail: _*)
  }

  def caloriesBurntBestToWorst(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.groupBy("user_id")
      .agg(sum("calories").as("calories"))
      .orderBy(col("calories").desc)
  }

  def activityUsedBestToWorstAmongFemales(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.filter("gender == 'F'")
      .groupBy("activity")
      .agg(approx_count_distinct(col("user_id")).as("count_of_users"))
      .orderBy(col("count_of_users").desc)
  }

  def activityUsedBestToWorstAmongFemalesSQLStyle(sparkSession: SparkSession, dataset: Dataset[Row]): Dataset[Row] = {
    dataset.createOrReplaceTempView("data")

    sparkSession.sql(
      """SELECT activity, COUNT(DISTINCT user_id) AS count_of_users
        |FROM data
        |WHERE gender = 'F'
        |GROUP BY activity
        |ORDER BY count_of_users DESC""".stripMargin)
  }
}

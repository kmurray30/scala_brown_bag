package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private var spark: SparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.sql.session.timeZone", "UTC") // Spark by default writes in the local time-zone, this tells spark to output in UTC
    .config("spark.broadcast.compress", "false")
    .config("spark.shuffle.compress", "false")
    .config("spark.shuffle.spill.compress", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  def main(args:Array[String]): Unit = {
    Console.println("Hello World!")

    val df = spark.read.csv("src\\test\\resources\\Data\\audit-data.txt")
    df.printSchema()
    df.show(5)
    var dfArray = df.collect()
    for(i <- 1 to 5){
      println(dfArray(i))
    }

    //df.select
  }
}
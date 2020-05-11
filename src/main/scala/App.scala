package org.example

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.TraversableOnce

/**
 * Hello world!
 *
 */
object App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val spark: SparkSession = SparkSession
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

    // todo flatten and map https://www.geeksforgeeks.org/scala-flatmap-method/
//    val name = Seq("Nidhi", "Singh")
//    // Applying map()
//    val result1 = name.map(_.toLowerCase)
//
//    // Output
//    List(nidhi, singh)
//
//    // Applying flatten() now,
//    val result2 = result1.flatten
//
//    // Output
//    List(n, i, d, h, i, s, i, n, g, h)

//    name.flatMap(_.toLowerCase)
//
//    // Output
//    List(n, i, d, h, i, s, i, n, g, h)










    // note could read as json, but this will give us some practice
    var df = spark.read.csv("src\\test\\resources\\Data\\audit-data.txt")
    var origDf = df
    df.printSchema()
    df.show(5)
    val dfArray = df.collect() // Note just for testing purposes. This is a super expensive way to do this
    for(i <- 1 to 5){
      println("row" + i + ": " + dfArray(i))
      // todo select column to print
      println("first column of row" + i + ": " + dfArray(i).toSeq(0))
      println("second column of row" + i + ": " + dfArray(i).toSeq(1))
    }

    // todo add computed column
    var newDf = df.withColumn("_c0-copy", col("_c0"))
    newDf.printSchema()
    println("New column contents: " + newDf.collect().last.toSeq.last)

    // todo extract column contents (ask for ideas? need udf)
    println("\n-----------------------------------------------------------\n")

    newDf = df.withColumn("CreationTime", getColumnContentsUdf(col("_c0")))
    println("New column name: " + newDf.schema.last.name)
    newDf.show(5)
    println("New column contents: " + newDf.collect().last.toSeq.last) // Note just for testing purposes. This is a super expensive way to do this

    // todo foreach extraction for all columns
    newDf = df
        .withColumn("UserId", getColumnContentsUdf(col("_c11")))
        .withColumn("Operation", getColumnContentsUdf(col("_c2")))
        .withColumn("ItemType", getColumnContentsUdf(col("_c14")))
        .select("UserId", "Operation", "ItemType")
    newDf.printSchema()
    newDf.collect().foreach(println)

    // todo enrich
    val recipientInfoLookupTable = spark.read
      .json("src\\test\\resources\\Data\\sample-recipient-lookup-table.txt")
    recipientInfoLookupTable.show()

    //var userInfo = spark.read.text("src\\test\\resources\\Data\\sample-recipient-lookup-table.txt")
    var joinedInfo = newDf.join(recipientInfoLookupTable, col("UserId") === recipientInfoLookupTable("Upn"),
    "left_outer")
    joinedInfo.show(15)

    // todo filter
    var filteredInfo = joinedInfo.filter(joinedInfo("Name") !== "null")
    filteredInfo.show(5)

    // todo agg
    filteredInfo
      .groupBy("Name")
      .agg(count("Operation") as "count")
      .orderBy("count")
      .show()

    filteredInfo
      .filter(filteredInfo("Name") === "John Dow")
      .groupBy("Name", "Operation")
      .agg(count("Operation") as "count")
      .orderBy(desc("count"))
      .show()

    // todo options, let's say the split fails sometimes and I want it to just ignore the column in that case

    // todo map and flatten and fold

    //origDf.flatMap(row => TraversableOnce)
  }

  val getColumnContentsUdf: UserDefinedFunction = udf(parseContents _)

  def parseContents(contents: String) {//: Option[String] = {
    // note set breakpoint here, call show(5) above, and demonstrate how it gets hit 5 times
    // not start without try catch, let it fail, then do try catch, then later convert to option
    try {
      contents.split('"')(3)
    } catch {
      case _: Exception => "failed"
    }
    //new Option(contents.split('"')(3), null)

  }

  // Note include this in the orig
  case class RecipientInfo(
                            Department: String,
                            JobTitle: String,
                            Location: String,
                            RedactedName: String,
                            Name: String,
                            RedactedUpn: String,
                            Upn: String,
                            ClassfnTitle: String
                          )
}
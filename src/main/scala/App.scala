package org.example

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.TraversableOnce
import scala.util.{Failure, Success, Try}

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

    val asdf = betterLowerRemoveAllWhitespace("ASDF ASDF")
    println(asdf)
    println(asdf.getOrElse())
    val asdf2 = betterLowerRemoveAllWhitespace(null)
    println(asdf2.getOrElse())
    println(None)

    val dots = "a1.b2.c3.d4"
    println(tryGetNth(dots, 3))
    tryGetNth(dots, 5) match {
      case Success(i) => println(i)
      case Failure(e) => println(s"Failed with $e")
    }

    println(
      optionGetNth(dots, 3)
        .map(s => s.toUpperCase() // lambda expression
          .concat("*"))
        .getOrElse("index out of bounds")
    )
    println(
      optionGetNth(dots, 5)
        .map(s => s.toUpperCase()
          .concat("*"))
        .getOrElse("index out of bounds")
    )

    // no such thing as operators
    val a : Int = 5
    println(a + 2)
    val num : WeirdNum = new WeirdNum(6);
    println(num + 2)
    println(num %^&* 2)
    println(num times 2) // wouldn't actually use methods like this

    // implicits
    val num2 : WeirdNum = 5;
    println(num2 + 2)

 // DATAFRAMES
 // https://www.youtube.com/watch?v=gj9g8QSyssQ
    import spark.implicits._
    val sampleDf = Seq(
      (10, "cat", "new york"),
      (4, "dog", "durham"),
      (7, null, "new delhi")
    ).toDF("num", "animal", "city")

    sampleDf
      .withColumn("city_starts_with_new", sampleDf("city").startsWith("new"))
      .withColumn("less than 5", col("num").lt(5))
      .withColumn("num plus five", col("num") + 5) // remember this is .+(5), calling the col method
      //.withColumn("num plus five", 5 + col("num")) // need to convert to lit(5)
      .withColumn("animal first two upper", upper(col("animal").substr(0, 2)))
      .show()

    sampleDf
      .filter(col("animal").isNotNull)
      .show()

    sampleDf
      .withColumn(
        "word_comparison",
        when(col("animal").substr(0, 1) === col("city").substr(0, 1), "starts with same")
          .when(lit(true), "override") // else if
          .otherwise("nope")
      ).show()

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
        .withColumn("ItemType", parseContents2(col("_c14"))) // better if possible
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

    // todo flatten and map https://www.geeksforgeeks.org/scala-flatmap-method/
    // todo options, let's say the split fails sometimes and I want it to just ignore the column in that case
  }

//  val getColumnContentsUdf: UserDefinedFunction = udf(parseContents _)
  val getColumnContentsUdf = udf[String, String](parseContents)

  def parseContents(contents: String) : String = {
    // note set breakpoint here, call show(5) above, and demonstrate how it gets hit 5 times
    // not start without try catch, let it fail, then do try catch, then later convert to option
    try {
      contents.split('"')(3)
    } catch {
      case _: Exception => "null"
    }
  }

  // https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b
  def parseContents2(col: Column) : Column = {
    try {
      split(col, "\"")(3) // use spark defined function
    } catch {
      case _: Exception => lit("null")
    }
  }

  def betterLowerRemoveAllWhitespace(s: String): Option[String] = {
    val str = Option(s).getOrElse(return None) // roundabout null check
    Some(str.toLowerCase().replaceAll("\\s", ""))
  }

  def tryGetNth(s: String, n: Int) : Try[String] = {
    Try(s.split('.')(n - 1))
  }

  def optionGetNth(s: String, index: Int) : Option[String] = {
    val split = s.split('.')
    if (index < split.length) {
      Some(split(index))
    }
    else {
      None
    }
  }

  class WeirdNum (num: Int) {
    def +(num2: Int) : Int = {
      num * num2
    }

    def %^&*(num2: Int) : Int = {
      num * num2
    }

    def times(num2: Int) : Int = {
      num * num2
    }
  }

  implicit def convertIntToWeirdNum(int : Int) : WeirdNum = {
    new WeirdNum(int)
  }
}
package com.github.mrpowers.my.cool.project

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession

case class OffenceCode(CODE: String, NAME: String)

object CrimeInBoston {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("crime_in_boston").getOrCreate()
    import spark.implicits._

    //    val crimes = spark.read.csv(args(0))
    val offenceCodes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/offense_codes.csv" /*args(1)*/)
      .as[OffenceCode]
      .map(row => OffenceCode("%05d".format(row.CODE.toInt), row.NAME))
      .show()
    //    val outputFile = args(2)

  }

  //  crimes
  //    .join(offenceCodes, crimes("OFFENSE_CODE") === offenceCodes.col("code"))
  //    .show()
  //    ..map(json => parse(json).extract[Wine])
  //    .foreach(wine => println(wine))

}

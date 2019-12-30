package com.github.mrpowers.my.cool.project

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

case class OffenceCode(CODE: String, NAME: String)
case class CrimeTypeRow(district: String, crime_type: String, count: Long)

object CrimeInBoston {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("crime_in_boston").getOrCreate()
    import spark.implicits._

    val crimes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    val offenceCodes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))
      .as[OffenceCode]
      .map(row => OffenceCode(
        "%05d".format(row.CODE.toInt),
        row.NAME.replaceAll("\"", "").split(" - ")(0))
      )

    val fullCrimeTable = crimes
      .join(broadcast(offenceCodes), crimes("OFFENSE_CODE") === offenceCodes.col("CODE"))
      .filter($"DISTRICT" =!= "U")
      .withColumnRenamed("DISTRICT", "district")
      .withColumnRenamed("NAME", "crime_type")
      .withColumnRenamed("INCIDENT_NUMBER", "incident_number")
      .cache()

    val crimesTotalAvgLatLong = fullCrimeTable
      .groupBy($"district")
      .agg(
        count("incident_number").as("crimes_total"),
        avg("Lat").as("lat"),
        avg("Long").as("lng")
      )
      .withColumn("crimes_monthly", round($"crimes_total" / 12, 2))

    val crimeTypes = fullCrimeTable
      .select("incident_number", "district", "crime_type")
      .groupBy("district", "crime_type")
      .count()
      .as[CrimeTypeRow]
      .groupByKey(_.district)
      .mapGroups {
        case (row, iter) =>
          val types = iter.toList.sortBy(-_.count).take(3)
            .map(_.crime_type)
            .mkString(", ")
          CrimeTypeRow(row.distinct, types, 0)
      }
      .withColumnRenamed("crime_type", "frequent_crime_types")
      .select("district", "frequent_crime_types")

    val result = crimeTypes
      .join(crimesTotalAvgLatLong, crimeTypes("district") === crimesTotalAvgLatLong("district"))
      .select(crimeTypes("district"), $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

    result.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args(2))
  }
}

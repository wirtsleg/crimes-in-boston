package com.github.mrpowers.my.cool.project

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

case class OffenceCode(CODE: String, NAME: String)

case class CrimeTypeRow(district: String, crime_type: String)

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
      .dropDuplicates("CODE")

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

    val months = fullCrimeTable
      .groupBy($"district", $"MONTH")
      .agg(count("incident_number").as("crimes_in_month"))

    months.createOrReplaceTempView("df")
    val medians = spark.sql(
      """
                           SELECT district as m_district, percentile_approx(crimes_in_month, 0.5) as crimes_monthly
                           FROM df GROUP BY district""")

    val crimesWithMedians = crimesTotalAvgLatLong
      .join(medians, crimesTotalAvgLatLong("district") === medians("m_district"))
      .drop("m_district")

    val windowSpec = Window.partitionBy($"district").orderBy($"count".desc)

    val crimeTypes = fullCrimeTable
      .select("incident_number", "district", "crime_type")
      .groupBy("district", "crime_type")
      .count()
      .withColumn("row_number", row_number.over(windowSpec))
      .filter($"row_number" <= 3)
      .drop("row_number", "count")
      .as[CrimeTypeRow]
      .groupByKey(_.district)
      .reduceGroups((x, y) => CrimeTypeRow(x.district, x.crime_type + ", " + y.crime_type))
      .map({ case (_, row) => row })
      .withColumnRenamed("crime_type", "frequent_crime_types")

    val result = crimeTypes
      .join(crimesWithMedians, crimeTypes("district") === crimesWithMedians("district"))
      .select(crimeTypes("district"), $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

    result.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args(2))
  }
}

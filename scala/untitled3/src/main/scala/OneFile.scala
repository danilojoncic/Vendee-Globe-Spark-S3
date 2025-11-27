import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, TimestampType}


object OneFile {
  def main(args: Array[String]): Unit = {

    val spark = Configurer.dockerSparkEnvironment("One file")

    val filePath = "s3a://csv/leaderboard_20241110_220000.csv"

    var df = spark.read
      .option("header", true)
      .option("inferSchema",true)
      .csv(filePath)

    val fileName = filePath.split("/").last // "leaderboard_20241110_220000.csv"
    val datePart = fileName.substring(12, 20) // "20241110"
    val splitSkipper = split(col("skipper_boat"), " ")
    val splitCol = split(col("nationality_sail"), " ")
    val dmToDecimalUDF = udf(dmToDecimal _)


    df = df
      .withColumn(
        "Sailor",
        when(col("skipper_boat").startsWith("Jean Le"), lit("Jean Le Cam"))
          .when(col("skipper_boat").startsWith("Denis Van"), lit("Denis Van Weynbergh"))
          .otherwise(concat_ws(" ", splitSkipper.getItem(0), splitSkipper.getItem(1)))
      )
      .withColumn(
        "Team",
        when(col("skipper_boat").startsWith("Jean Le Cam"),
          trim(substring(col("skipper_boat"), 13, 1000))
        )
          .when(col("skipper_boat").startsWith("Denis Van"),
            trim(substring(col("skipper_boat"), 19, 1000))
          )
          .otherwise(concat_ws(" ", slice(splitSkipper, 3, 1000)))
      )
      .withColumn(
        "Nation",
        splitCol.getItem(0))
      .withColumn(
        "Sail",
        concat_ws(" ", splitCol.getItem(1), splitCol.getItem(2)))
      .withColumn(
        "Time in France",
        to_timestamp(concat(lit(datePart), lit(" "), substring(col("time"), 0, 5)), "yyyyMMdd HH:mm")
      )
      .withColumn("speed_30min",   regexp_replace(col("speed_30min"), " kts", ""))
      .withColumn("avg_speed_30min", regexp_replace(col("avg_speed_30min"), " kts", ""))
      .withColumn("speed_last_report", regexp_replace(col("speed_last_report"), " kts", ""))
      .withColumn("avg_speed_last_report", regexp_replace(col("avg_speed_last_report"), " kts", ""))
      .withColumn("speed_24h", regexp_replace(col("speed_24h"), " kts", ""))
      .withColumn("avg_speed_24h", regexp_replace(col("avg_speed_24h"), " kts", ""))
      .withColumn("distance_30min", regexp_replace(col("distance_30min"), " nm", ""))
      .withColumn("distance_last_report", regexp_replace(col("distance_last_report"), " nm", ""))
      .withColumn("distance_24h", regexp_replace(col("distance_24h"), " nm", ""))
      .withColumn("dtf", regexp_replace(col("dtf"), " nm", ""))
      .withColumn("dtl", regexp_replace(col("dtl"), " nm", ""))
      .withColumn("heading_30min", regexp_replace(col("heading_30min"), "°", ""))
      .withColumn("heading_last_report", regexp_replace(col("heading_30min"), "°", ""))
      .withColumn("heading_24h", regexp_replace(col("heading_30min"), "°", ""))
      .withColumn("latitude", dmToDecimalUDF(col("latitude")))
      .withColumn("longitude", dmToDecimalUDF(col("longitude")))


      .drop("nationality_sail","skipper_boat","time")



    df = df
      .withColumn("rank", col("rank").cast(IntegerType))
      .withColumn("heading_30min", col("heading_30min").cast(IntegerType))
      .withColumn("speed_30min", col("speed_30min").cast(FloatType))
      .withColumn("avg_speed_30min", col("avg_speed_30min").cast(FloatType))
      .withColumn("distance_30min", col("distance_30min").cast(FloatType))
      .withColumn("heading_last_report", col("heading_last_report").cast(IntegerType))
      .withColumn("speed_last_report", col("speed_last_report").cast(FloatType))
      .withColumn("avg_speed_last_report", col("avg_speed_last_report").cast(FloatType))
      .withColumn("distance_last_report", col("distance_last_report").cast(FloatType))
      .withColumn("heading_24h", col("heading_24h").cast(IntegerType))
      .withColumn("speed_24h", col("speed_24h").cast(FloatType))
      .withColumn("avg_speed_24h", col("avg_speed_24h").cast(FloatType))
      .withColumn("distance_24h", col("distance_24h").cast(FloatType))
      .withColumn("dtf", col("dtf").cast(FloatType))
      .withColumn("dtl", col("dtl").cast(FloatType))
      .withColumn("Time in France", col("Time in France").cast(TimestampType))

    df.show(50)
    df.printSchema()



    spark.stop()
  }


  /**
   * // Latitude
   * val dfDecimal = df
   * .withColumn("latitude",
   * when(substring(col("latitude"), -1, 1) === "S",
   * -(substring_index(col("latitude"), "°", 1).cast("float") +
   * substring(col("latitude"), instr(col("latitude"), "°") + 1, length(col("latitude"))-instr(col("latitude"), "°")-1).cast("float")/60))
   * .otherwise(
   * substring_index(col("latitude"), "°", 1).cast("float") +
   * substring(col("latitude"), instr(col("latitude"), "°") + 1, length(col("latitude"))-instr(col("latitude"), "°")-1).cast("float")/60)
   * )
   *
   * // Longitude
   * .withColumn("longitude",
   * when(substring(col("longitude"), -1, 1) === "W",
   * -(substring_index(col("longitude"), "°", 1).cast("float") +
   * substring(col("longitude"), instr(col("longitude"), "°") + 1, length(col("longitude"))-instr(col("longitude"), "°")-1).cast("float")/60))
   * .otherwise(
   * substring_index(col("longitude"), "°", 1).cast("float") +
   * substring(col("longitude"), instr(col("longitude"), "°") + 1, length(col("longitude"))-instr(col("longitude"), "°")-1).cast("float")/60)
   * )
   */

  def dmToDecimal(coord: String): Float = {
    val degMin = coord.split("°")
    val deg = degMin(0).toFloat
    val minH = degMin(1)

    // last character is hemisphere
    val hemi = minH.last

    // remove any non-digit/non-dot characters from minutes
    val min = minH.takeWhile(c => c.isDigit || c == '.').toFloat

    val decimal = deg + min / 60.0f
    if (hemi == 'S' || hemi == 'W') -decimal else decimal
  }

}

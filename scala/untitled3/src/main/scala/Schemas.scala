import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}

object Schemas {
  val baseSchema = StructType(Seq(
    StructField("rank", IntegerType, true),
    StructField("latitude", FloatType, true),
    StructField("longitude", FloatType, true),
    StructField("heading_30min", FloatType, true),
    StructField("speed_30min", FloatType, true),
    StructField("avg_speed_30min", FloatType, true),
    StructField("distance_30min", FloatType, true),
    StructField("heading_last_report", FloatType, true),
    StructField("speed_last_report", FloatType, true),
    StructField("avg_speed_last_report", FloatType, true),
    StructField("distance_last_report", FloatType, true),
    StructField("heading_24h", FloatType, true),
    StructField("speed_24h", FloatType, true),
    StructField("avg_speed_24h", FloatType, true),
    StructField("distance_24h", FloatType, true),
    StructField("dtf", FloatType, true),
    StructField("dtl", FloatType, true),
    StructField("Sailor", StringType, true),
    StructField("Team", StringType, true),
    StructField("Nation", StringType, true),
    StructField("Sail", StringType, true),
    StructField("Time in France", TimestampType, true)
  ))

  val conditionsSchema = StructType(Seq(

  ))

}

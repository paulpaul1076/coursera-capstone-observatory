package observatory

import java.time.LocalDate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.avg

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val stationsSchema = StructType(Array(
    StructField(name = "stn_id2", dataType = StringType, nullable = true),
    StructField(name = "wban_id2", dataType = StringType, nullable = true),
    StructField(name = "latitude", dataType = DoubleType, nullable = true),
    StructField(name = "longitude", dataType = DoubleType, nullable = true)
  ))

  import spark.implicits._

  lazy val stationsFileStream = Source.getClass.getResourceAsStream("/stations.csv")
  lazy val stations = spark.read.schema(stationsSchema).csv(spark.sparkContext.parallelize(Source.fromInputStream(stationsFileStream).getLines().toSeq, 1000).toDS()).filter($"latitude".isNotNull.and($"longitude".isNotNull))
  lazy val stations2 = stations.map(row => ((row.getAs[String]("stn_id2"), row.getAs[String]("wban_id2")), row.getAs[Double]("latitude"), row.getAs[Double]("longitude"))).toDF("composite", "latitude", "longitude").cache()

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val temperaturesSchema = StructType(Array(
      StructField(name = "stn_id", dataType = StringType, nullable = true),
      StructField(name = "wban_id", dataType = StringType, nullable = true),
      StructField(name = "month", dataType = IntegerType, nullable = true),
      StructField(name = "day", dataType = IntegerType, nullable = true),
      StructField(name = "temperature", dataType = DoubleType, nullable = true)
    ))


    import spark.implicits._

    val temperatureFileStream = Source.getClass.getResourceAsStream(temperaturesFile)
    val temperatures = spark.read.schema(temperaturesSchema).csv(spark.sparkContext.parallelize(Source.fromInputStream(temperatureFileStream).getLines().toSeq, 10000).toDS())
    val temperatures2 = temperatures.map(row => ((row.getAs[String]("stn_id"), row.getAs[String]("wban_id")), row.getAs[Int]("month"), row.getAs[Int]("day"), row.getAs[Double]("temperature"))).toDF("composite", "month", "day", "temperature")
    val joined = temperatures2.join(stations2, temperatures2("composite") === stations2("composite"))

    def fahrenheitToCelcius(fahrenheit: Double): Double = {
      (fahrenheit - 32.0) * 5.0 / 9.0
    }

    joined
      .map(row => (
        year,
        row.getAs[Int]("month"),
        row.getAs[Int]("day"),
        Location(row.getAs[Double]("latitude"), row.getAs[Double]("longitude")),
        fahrenheitToCelcius(row.getAs[Double]("temperature"))
      )).collect()
      .map{
        case (year, month, day, location, temperature) =>
          (LocalDate.of(year, month, day), location, temperature)
      }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    import spark.implicits._

    val recordsDF = records.map {
      case (_, location, temperature) =>
        (location, temperature)
    }.toSeq.toDF("location", "temperature")

    recordsDF
      .groupBy($"location").agg(avg($"temperature").as("temperature"))
      .select($"location", $"temperature").as[(Location, Double)]
      .collect()
  }
}
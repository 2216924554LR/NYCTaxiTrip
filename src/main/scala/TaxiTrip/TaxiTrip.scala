package TaxiTrip

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.Test

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

object TaxiTrip {

  def main(args: Array[String]): Unit = {

//    create sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

//    import packages
    import spark.implicits._
    import org.apache.spark.sql.functions._

//    read dataset
    val taxiRaw: Dataset[Row] = spark.read
      .option("header", value = true)
      .csv("dataset/taxi_5000.csv")

//    taxiRaw.show(10)
//    taxiRaw.printSchema()

    val taxiParsed = taxiRaw.rdd.map(safe(parse))

//     get all error rows
//    val errorRow = taxiParsed.filter(e => e.isRight)
//      .map(e => e.right.get._1)
//
//    errorRow.collect().foreach(print(_))



    val taxiClean = taxiParsed.map(either => either.left.get).toDS()
//    taxiClean.show(10)


//   draw a trip time bar histogram
//    1. UDF function
    val hours = (pickUpTime:Long, dropOffTime:Long) => {
      val duration = dropOffTime - pickUpTime
      val hours = TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
      hours
    }
    val hoursUDF = udf(hours)
//    2. make statistics
    taxiClean.groupBy(hoursUDF($"pickUpTime", $"dropOffTime") as "duration")
      .count()
      .sort("duration")
      .show()

//    delete abnormal data

    spark.udf.register("hours", hours)
    val taxiClean2 = taxiClean.where("hours(pickUpTime, dropOffTime) between 0 and 3")
    taxiClean2.show(10)

  }

  /**
   * Packaging parse method and catch error
   */
  def safe[P, R](f: P =>R): P => Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {
      override def apply(param: P): Either[R, (P, Exception)] = {
        try{
          Left(f(param))
        } catch {
          case e: Exception => Right((param ,e))
        }
      }
    }
  }


  def parse(row: Row): Trip ={
    val richRow = new RichRow(row)
    val id = richRow.getAs[String]("id").orNull
    val pickUpTime = parseTime(richRow, "pickup_datetime")
    val dropOffTime = parseTime(richRow, "dropoff_datetime")
    val pickUpX = parseLocation(richRow, "pickup_longitude")
    val pickUpY = parseLocation(richRow, "pickup_latitude")
    val dropOffX = parseLocation(richRow, "dropoff_longitude")
    val dropOffY = parseLocation(richRow, "dropoff_latitude")

    Trip(id, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }

  def parseTime(row: RichRow, field: String):Long = {
    // SimpleDataFormat
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)
    // getTime
    val time = row.getAs[String](field)
    val timeOption = time.map(time => formatter.parse(time).getTime)
    timeOption.getOrElse(0L)
  }

  def parseLocation(row: RichRow, field: String):Double = {
    val location = row.getAs[String](field)
    val locationOption = location.map(loc => loc.toDouble)
    locationOption.getOrElse(0.0D)
  }
}

class RichRow(row: Row){
  def getAs[T](field:String):Option[T] ={
    // 1. whether row.getAs is empty
    if (row.isNullAt(row.fieldIndex(field))){
      // 2. null -> None
      None
    }else{
      // 3. not null -> Some
      Some(row.getAs[T](field))
    }
  }
}


case class Trip(
      id: String,
      pickUpTime:  Long,
      dropOffTime: Long,
      pickUpX: Double,
      pickUpY: Double,
      dropOffX: Double,
      dropOffY: Double
               )

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.DoubleType


object MainObject {
  def main(args:Array[String]) : Unit = {

    Logger.getRootLogger().setLevel(Level.WARN)
    //System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master\\")
//    val df1 = System.setProperty("spark.sql.warehouse.dir","file:///C:/Users/xps/IdeaProjects/Prediction_US_Department_of_Transportation/2007.csv")

    //C:/Users/xps/IdeaProjects/Prediction_US_Department_of_Transportation/2007.csv
    println("Please, Enter your data file path: ")
    val filePath =scala.io.StdIn.readLine()
    println("Please, Enter your file name with extension: ")
    val fileName =scala.io.StdIn.readLine()
    println("Please, Enter the airport.csv file path: ")
    val airportfilePath =scala.io.StdIn.readLine()


    val conf = new SparkConf().setAppName("PredictionFlightsDelayUSTransportation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("PredictionFlightsDelayUSTransportation").config("spark.sql.warehouse.dir", "file:///$filePath").getOrCreate()
    val targetedVariable= "ArrDelay"
//    val sqlContext = new SQLContext(sc)

    var flight = new Flights(spark,targetedVariable)
    flight.variablesTransformation(flight.dataClean(flight.variablesSelection(flight.dataLoad(fileName))))

    val spark1 = SparkSession.builder().appName("PredictionFlightsDelayUSTransportation").config("spark.sql.warehouse.dir", "file:///$airportfilePath").getOrCreate()
    var airportsDF = spark1.read.option("header", "true").csv("airports.csv")
    // Adding new Variables: lat and long of each airport
    airportsDF  = airportsDF.select(airportsDF.col("iata"),airportsDF.col("lat").cast(DoubleType),airportsDF.col("long").cast(DoubleType))
    // Adding New columns: lat and long of the Origin airports
    flight.df = flight.df.join(airportsDF,flight.df.col("Origin") === airportsDF.col("iata")).withColumnRenamed("lat","OriginLat").withColumnRenamed("long","OriginLong").drop("iata")
    // Adding New columns: lat and long of the Destination airports
    flight.df = flight.df.join(airportsDF,flight.df.col("Dest") === airportsDF.col("iata")).withColumnRenamed("lat","DestLat").withColumnRenamed("long","DestLong").drop("iata")
    //Eliminate useless variables
    flight.df = flight.df.drop("Cancelled").drop("CancellationCode").drop("Origin").drop("Dest").drop("Year").drop("DayOfMonth").drop("FlightNum").drop("TailNum").drop("UniqueCarrier")
    //Eliminate all the null values in any column and it doesn't present more than 1% of the total data
    flight.df = flight.df.na.drop()

    flight.df.show(10)






    //val df = spark.read.option("header", "true").csv(fileName)



    //val df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "true").load("file:///C:/Users/xps/IdeaProjects/Prediction_US_Department_of_Transportation/2007.csv")

    //val partationedDF = df//.repartition(3)
    //sqlContext.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header","true").schema(schema).load("D:\\Spain Trip\\UPM Classes\\Big Data\\Assignment\\2007\\2007.csv")
    //(com.databricks.spark.csv.DefaultSource15, org.apache.spark.sql.execution.datasources.csv.CSVFileFormat)
     // val parquet = sqlContext.read.parquet("C:/Users/xps/IdeaProjects/Prediction_US_Department_of_Transportation/2007.csv.parquet")

    //val allowedVariablesDF  = df.select("Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","CRSElapsedTime","ArrDelay","DepDelay","Origin","Dest","Distance","TaxiOut","Cancelled","CancellationCode")
    //allowedVariablesDF.col("Year").cast(String)
    ///val targetedVariablesDF = allowedVariablesDF.select("ArrDelay")


    ///allowedVariablesDF.show(10)
    ///targetedVariablesDF.sample(true,5).show(5)

    ///allowedVariablesDF.select("Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","CRSElapsedTime","ArrDelay","DepDelay","Origin","Dest","Distance","TaxiOut","Cancelled","CancellationCode").distinct().show()


    // println("the max group by Month and Day of Month"+allowedVariablesDF.groupBy("Month","DayofMonth").max())











    /*
    val tf = sc.textFile(args(0))
    val splits = tf.flatMap(line => line.split(" ")).map(word =>(word,1))
    val counts = splits.reduceByKey((x,y)=>x+y)
    splits.saveAsTextFile(args(1))
    counts.saveAsTextFile(args(2))
    */
    println("Hello, word count")

  }

}





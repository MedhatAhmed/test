import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.StringIndexer

//import org.apache.spark.implicits._


class Flights (spark : SparkSession , targetedVariablesDF : String){
      var df : DataFrame = null

  def dataLoad(fileName : String) : DataFrame =
  {
  //  val allowedVariablesDF  = df.select("Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","CRSElapsedTime","ArrDelay","DepDelay","Origin","Dest","Distance","TaxiOut","Cancelled","CancellationCode")
    // val d = allowedVariablesDF.select( col = "Year" ).asInstanceOf[Double]

    //Load the data from the file
    df = spark.read.option("header", "true").csv(fileName)
    //Select useful variables
    df
  }

  //Select useful variables and Transform the data
  // need to cast the default to String
  def variablesSelection(dataDF: DataFrame) : DataFrame =
  {
    var allowedVariablesDF = dataDF.select( "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "CRSElapsedTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiOut", "Cancelled", "CancellationCode" )
    //var x = df.select(df.col("Year").cast(StringType) , df.col("DayOfWeek").cast(DoubleType))


    allowedVariablesDF = allowedVariablesDF.select(
      allowedVariablesDF.col("Year"),allowedVariablesDF.col("Month"),allowedVariablesDF.col("DayofMonth"),
      allowedVariablesDF.col("DayOfWeek").cast(DoubleType),allowedVariablesDF.col("DepTime").cast(DoubleType),allowedVariablesDF.col("CRSDepTime"),
      allowedVariablesDF.col("CRSArrTime"),allowedVariablesDF.col("UniqueCarrier"),allowedVariablesDF.col("FlightNum"),
      allowedVariablesDF.col("TailNum"),allowedVariablesDF.col("CRSElapsedTime").cast(DoubleType),allowedVariablesDF.col("ArrDelay").cast(DoubleType),
      allowedVariablesDF.col("DepDelay").cast(DoubleType),allowedVariablesDF.col("Origin"),allowedVariablesDF.col("Dest"),
      allowedVariablesDF.col("Distance").cast(DoubleType),allowedVariablesDF.col("TaxiOut").cast(DoubleType),allowedVariablesDF.col("Cancelled").cast(DoubleType),
      allowedVariablesDF.col("CancellationCode")
    )

    df = allowedVariablesDF
    allowedVariablesDF
  }

  //remove duplicates and na variables and there were many records containing null values in ArrDelay attribute
  def dataClean(allowedVariablesDF: DataFrame) : DataFrame =
  {
    var cleanedDF = allowedVariablesDF.dropDuplicates()
    cleanedDF = cleanedDF.na.drop(Array("ArrDelay"))
    ///cleanedDF.show(10)
    df = cleanedDF
    cleanedDF
  }

  // Transform a column date in "dd/MM/yyyy HHmm" format to a Unix TimeStamp column
  def dateToTimeStamp(dttdf: DataFrame, colName: String) : DataFrame =
  {
    return dttdf.withColumn(colName,
      unix_timestamp(concat(col("DayOfMonth"), lit("/"), col("Month"), lit("/"), col("Year"), lit(" "), col(colName)),
        "dd/MM/yyyy HHmm"))
  }

  def variablesTransformation(vtDF : DataFrame) : DataFrame =
  {

    df = dateToTimeStamp(vtDF, "CRSDepTime")
    df = dateToTimeStamp(df, "CRSArrTime")


    val timeStampReference = unix_timestamp(lit("01/01/1900"), "dd/MM/yy")

  ///-----------Revisit this part -----------------------------------------

    ///  df = df.withColumn("CRSDepTime", $"CRSDepTime" - timeStampReference)
  ///  df = df.withColumn("CRSArrTime", $"CRSArrTime" - timeStampReference)

    //Cast variables to Double due to machine learning methods restrictions.
    df = df.withColumn("DayOfMonth", col("DayOfMonth").cast(DoubleType))
    df = df.withColumn("CRSDepTime", col("CRSDepTime").cast(DoubleType))
    df = df.withColumn("CRSArrTime", col("CRSArrTime").cast(DoubleType))
    df = df.withColumn("Year", col("Year").cast(DoubleType))
    df = df.withColumn("Month", col("Month").cast(DoubleType))

    //StringIndexer to transform the UniqueCarrier string to integer for using it as a categorical variable.
    val sIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierInt")
    df = sIndexer.fit(df).transform(df)


    //df.show(10)
    df
  }








}


//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // --------------------------------------------
  // FUNCTION createDataFrameRowsImplicitSchema
  // --------------------------------------------
  def createDataFrameRowsImplicitSchema(sc: SparkContext, spark: SparkSession): Unit = {
    //1. We create the Row Objects
    val name1: Row = Row.fromTuple("Luis", "Johnson")
    val sports1: Array[Row] = Array(Row.fromTuple("Football", 10), Row.fromTuple("Basketball", 6), Row.fromTuple("Tennis", 5))
    val p1: Row = Row( name1, "Brown", "Madrid", 34, sports1 )

    val name2: Row = Row.fromTuple("John", "Rossi")
    val sports2: Array[Row] = Array(Row.fromTuple("Football", 8), Row.fromTuple("Basketball", 4))
    val p2: Row = Row( name2, "Blue", "Paris", 20, sports2 )

    val name3: Row = Row.fromTuple("Francesca", "Depardieu")
    val sports3: Array[Row] = Array(Row.fromTuple("Tennis", 8), Row.fromTuple("Basketball", 7))
    val p3: Row = Row( name3, "Blue", "London", 26, sports3 )

    val name4: Row = Row.fromTuple("Laurant", "Muller")
    val sports4: Array[Row] = Array(Row.fromTuple("Tennis", 1))
    val p4: Row = Row( name4, "Green", "Paris", 26, sports4 )

    val name5: Row = Row.fromTuple("Gertrud", "Gonzalez")
    val sports5: Array[Row] = Array(Row.fromTuple("Rugby", 10))
    val p5: Row = Row( name5, "Green", "Dublin", 32, sports5 )

    // 2. Operation C1: Creation 'parallelize'
    val inputRDD: RDD[Row] = sc.parallelize(Array(p1, p2, p3, p4, p5))

    // 3. We define the Schema of our DF.

    // 3.1. We define the datatype of the first field
    val nameRowField = StructType(List(StructField("name", StringType, true), StructField("surname", StringType, true)))

    // 3.2. We define the datatype of the fifth field
    val sportRowField = StructType(List(StructField("sport", StringType, true), StructField("score", IntegerType, true)))

    val sportRowsList = ArrayType(sportRowField, true)

    // 3.3. We put together all the schema
    val mySchema: StructType = StructType(List(StructField("identifier", nameRowField, true), StructField("eyes", StringType, true),
      StructField("city", StringType, true), StructField("age", IntegerType, true),
      StructField("likes", sportRowsList, true)))

    // 3. Operation C2: Creation 'createDataFrame'
    val inputDF: DataFrame = spark.createDataFrame(inputRDD, mySchema)

    // 4. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    // 5. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    // 6. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    val newDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

    // 7. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()
  }

  // --------------------------------------------
  // FUNCTION createDataFrameRDDExplicitShema
  // --------------------------------------------
  def createDataFrameRDDExplicitShema(sc: SparkContext, spark: SparkSession): Unit = {
    // 1. Operation C1: Creation 'parallelize'
    val inputRDD: RDD[(String, Int)] = sc.parallelize(Array(("Madrid" , 34), ("Paris", 20), ("London", 26), ("Paris", 26), ("Dublin", 32)))

    // 2. Operation T1: Transformation to get valid format
    val formatRDD: RDD[Row] = inputRDD.map( (item: (String, Int)) => Row(item._1, item._2) )

    // 2. We define the Schema of our DF.
    val mySchema: StructType = StructType(List(StructField("city", StringType, true), StructField("age", IntegerType, true)))

    // 3. Operation C2: Creation 'createDataFrame'
    val inputDF: DataFrame = spark.createDataFrame(formatRDD, mySchema)

    // 4. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    // 5. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    // 6. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    val newDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

    // 7. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()
  }

  //-------------------------------------
  // FUNCTION myMain
  //-------------------------------------
  def myMain(sc: SparkContext, spark: SparkSession, option: Int): Unit = {
    option match {
      case 1  => println("\n\n--- DataFrame from Row Objects and Implicit Schema ---\n\n")
        createDataFrameRowsImplicitSchema(sc, spark)

      case 2  => println("\n\n--- DataFrame from RDD and Explicit Schema ---\n\n")
        createDataFrameRDDExplicitShema(sc, spark)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option: Int = 2

    // 2. Local or Databricks
    val localFalseDatabricksTrue = true

    // 3. We setup the log level
    val logger = org.apache.log4j.Logger.getLogger("org")
    logger.setLevel(Level.WARN)

    // 4. We configure the Spark Context sc
    var sc : SparkContext = null;

    // 4.1. Local mode
    if (localFalseDatabricksTrue == false){
      // 4.1.1. We create the configuration object
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("MyProgram")

      // 4.1.2. We initialise the Spark Context under such configuration
      sc = new SparkContext(conf)
    }
    // 4.2. Databricks
    else{
      sc = SparkContext.getOrCreate()
    }
    println("\n\n\n");

    // 5. We configure the Spark variable
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    println("\n\n\n");

    // 6. We call to myMain
    myMain(sc, spark, option)
    println("\n\n\n");
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

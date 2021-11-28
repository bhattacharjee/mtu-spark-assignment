
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.graphframes.lib.AggregateMessages
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.graphframes.GraphFrame

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION myAggregateMessages
  // ------------------------------------------
  def myAggregateMessages(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 1.5. Operation T2: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src")
      .withColumnRenamed("src", "id")
      .dropDuplicates()
      .withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation T4: We aggregate messages for myGF. In particular, for each v we aggregate the sum of all neighbour

    // 2.1. We create the aggregate object, which provides a kind of triplet view: src, edge, dst
    val myAO = AggregateMessages

    // 2.2. Operation T4: We aggregate the messages in a DataFrame
    // (1) The first parameter is the merge function: How do we aggregate all messages being sent?
    //     In this case it just uses the function sum to sum them all
    // (2) The second argument is the sendToSrc messages: Given each element of the triplets view: What message do we send per edge?
    //     In this case we just decide to send the id of the destination vertex
    // (3) The third argument is the sendToDst messages: Given each element of the triplets view: What message do we send per edge?
    //     In this case we just decide to send the id of the source vertex
    val myAggregatedDF = myGF.aggregateMessages
      .sendToSrc(myAO.dst("id"))
      .sendToDst(myAO.src("id"))
      .agg(sum(myAO.msg).as("result"))

    // 3. Operation A1: We display the content of myAggregatedDF
    myAggregatedDF.show()
  }

  // ------------------------------------------
  // FUNCTION myCollectNeighborIds
  // ------------------------------------------
  def myCollectNeighborIds(spark: SparkSession, myDatasetDir: String, direction: Boolean): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 1.5. Operation T2: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src")
      .withColumnRenamed("src", "id")
      .dropDuplicates()
      .withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation T4: We get the edges again
    var myEdges2DF = spark.createDataFrame(Array((1, 1), (2, 2))).toDF("id", "value")

    if (direction == true)
      myEdges2DF = myGF.edges.drop("value")
        .groupBy("src")
        .agg(collect_list("dst").alias("out_neighbors"))
    else
      myEdges2DF = myGF.edges.drop("value")
        .groupBy("dst")
        .agg(collect_list("src").alias("in_neighbors"))

    // 3. Operation A1: We display myEdgesDF
    myEdges2DF.show()
  }



  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(spark: SparkSession, myDatasetDir: String, option: Int, direction : Boolean): Unit = {
    option match {
      case 1  => myAggregateMessages(spark, myDatasetDir)
      case 2  => myCollectNeighborIds(spark, myDatasetDir, direction)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option : Integer = 2
    val direction : Boolean = true

    // 2. Local or Databricks
    val localFalseDatabricksTrue = true

    // 3. We set the path to my_dataset and my_result
    val myLocalPath = "../../"
    val myDatabricksPath = "/"

    var myDatasetDir : String = "FileStore/tables/4_Spark_Graph_Libs/1_TinyGraph/my_dataset"

    if (localFalseDatabricksTrue == false) {
      myDatasetDir = myLocalPath + myDatasetDir
    }
    else {
      myDatasetDir = myDatabricksPath + myDatasetDir
    }

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
    // 4.2. Databricks Mode
    else{
      sc = SparkContext.getOrCreate()
    }

    // 5. We configure the Spark variable
    var spark : SparkSession = SparkSession.builder().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n");
    }

    // 6. We call to myMain
    myMain(spark, myDatasetDir, option, direction)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

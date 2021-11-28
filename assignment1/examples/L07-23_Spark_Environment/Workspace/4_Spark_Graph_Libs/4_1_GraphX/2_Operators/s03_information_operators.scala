
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.graphframes.GraphFrame

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION getNumVertices
  // ------------------------------------------
  def getNumVertices(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 2. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src").withColumnRenamed("src", "id").dropDuplicates().withColumn("value", lit(1))

    // 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 4. Operation A1: We get the num of vertices
    val resVAL = myGF.vertices.count()

    // 5. We print the result
    printf("%d\n", resVAL)

  }

  // ------------------------------------------
  // FUNCTION getNumEdges
  // ------------------------------------------
  def getNumEdges(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 2. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src").withColumnRenamed("src", "id").dropDuplicates().withColumn("value", lit(1))

    // 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 4. Operation A1: We get the num of edges
    val resVAL = myGF.edges.count()

    // 5. We print the result
    printf("%d\n", resVAL)
  }

  // ------------------------------------------
  // FUNCTION inDegrees
  // ------------------------------------------
  def inDegrees(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 2. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src").withColumnRenamed("src", "id").dropDuplicates().withColumn("value", lit(1))

    // 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 4. Operation T1: We get the num of vertices in the form of a DF
    val solVerticesDF = myGF.inDegrees

    // 5. Operation A1: We print the content of sol_verticesDF
    solVerticesDF.show()
  }

  // ------------------------------------------
  // FUNCTION outDegrees
  // ------------------------------------------
  def outDegrees(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 2. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src").withColumnRenamed("src", "id").dropDuplicates().withColumn("value", lit(1))

    // 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 4. Operation T1: We get the num of vertices in the form of a DF
    val solVerticesDF = myGF.outDegrees

    // 5. Operation A1: We print the content of sol_verticesDF
    solVerticesDF.show()
  }


  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(spark: SparkSession, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => getNumVertices(spark, myDatasetDir)
      case 2  => getNumEdges(spark, myDatasetDir)
      case 3  => inDegrees(spark, myDatasetDir)
      case 4  => outDegrees(spark, myDatasetDir)
    }
  }


  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option : Integer = 4

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
    myMain(spark, myDatasetDir, option)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

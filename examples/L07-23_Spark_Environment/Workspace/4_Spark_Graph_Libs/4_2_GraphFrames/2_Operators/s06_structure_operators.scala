
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.graphframes.GraphFrame

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION myReverse
  // ------------------------------------------
  def myReverse(spark: SparkSession, myDatasetDir: String): Unit = {
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
    val myVerticesDF = myEdgesDF.select("src").withColumnRenamed("src", "id").dropDuplicates().withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation P2: We persist myGF
    myGF.persist()

    // 3. Operation T3: We transform the edges value
    val myNewEdgesDF = myGF.edges.withColumnRenamed("src", "aux")
      .withColumnRenamed("dst", "src")
      .withColumnRenamed("aux", "dst")

    // 4. Operation C3: We create myNewGF from my_vertices and myNewEdgesDF
    val myNewGF = GraphFrame(myGF.vertices, myNewEdgesDF)

    // 5. Operation T4: We get the solEdgesDF
    val solEdgesDF = myNewGF.edges

    // 5. Operation A1: We collect the DF
    val resVAL = solEdgesDF.collect()

    // 7. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myAddFunction
  // ------------------------------------------
  val myAddFunction: (Column, Column) => Column = (value, const) => {
    // 1. We create the output variable
    var res: Column = (value + const)

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION mySubgraph
  // ------------------------------------------
  def mySubgraph(spark: SparkSession, myDatasetDir: String): Unit = {
    // 1. We load my_edgesDF from the dataset

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("src", IntegerType, true), StructField("dst", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val myEdgesDatasetDF = spark.read.format("csv").option("delimiter", " ").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 1.3. Operation T1: We add the value column
    val myEdgesDF = myEdgesDatasetDF.withColumn("value", lit(1))
      .withColumn("aux", myAddFunction(col("src"), col("dst")))

    // 1.4. Operation P1: We persist it
    myEdgesDF.persist()

    // 1.5. Operation T2: We transform my_edgesDF so as to get my_verticesDF
    val myVerticesDF = myEdgesDF.select("src")
      .withColumnRenamed("src", "id")
      .dropDuplicates()
      .withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation P2: We persist myGF
    myGF.persist()

    // 3. Operation T3: We have to do this walk around as filterEdges only accepts a column, not two
    val myNewGF = myGF.filterEdges("aux % 2 == 1")
      .filterVertices("id > 3")
      .dropIsolatedVertices()

    // 4. Operation P2: We persist myNewGF
    myNewGF.persist()

    // 5. Operation T4: We get the solVerticesDF
    val solVerticesDF = myNewGF.vertices

    // 6. Operation A1: We display the DF
    solVerticesDF.show()

    // 7. Operation T5: We get the solEdgesDF
    val solEdgesDF = myNewGF.edges

    // 8. Operation A2: We display the DF
    solEdgesDF.show()
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(spark: SparkSession, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => myReverse(spark, myDatasetDir)
      case 2  => mySubgraph(spark, myDatasetDir)
    }
  }


  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option : Integer = 1

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

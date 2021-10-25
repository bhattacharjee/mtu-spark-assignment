
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.graphframes._
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  //-------------------------------------
  // FUNCTION myMain
  //-------------------------------------
  def myMain(spark: SparkSession, myDatasetDir: String): Unit = {
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

    // 4. Operation P1: We persist myGF
    myGF.persist()

    // 5. Operation T4: We revert back to sol_verticesDF
    val solVerticesDF = myGF.vertices

    // 6. Operation A1: We display sol_verticesDF
    solVerticesDF.show()

    // 7. Operation T5: We revert back to sol_edgesDF
    val solEdgesDF = myGF.edges

    // 8. Operation A2: We display sol_edgesDF
    solEdgesDF.show()
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. Local or Databricks
    val localFalseDatabricksTrue = true

    // 2. We set the path to my_dataset and my_result
    val myLocalPath = "../../"
    val myDatabricksPath = "/"

    var myDatasetDir : String = "FileStore/tables/4_Spark_Graph_Libs/1_TinyGraph/my_dataset"

    if (localFalseDatabricksTrue == false) {
      myDatasetDir = myLocalPath + myDatasetDir
    }
    else {
      myDatasetDir = myDatabricksPath + myDatasetDir
    }

    // 3. We configure the Spark Context sc
    var sc : SparkContext = null;

    // 3.1. Local mode
    if (localFalseDatabricksTrue == false){
      // 3.1.1. We create the configuration object
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("MyProgram")

      // 3.1.2. We initialise the Spark Context under such configuration
      sc = new SparkContext(conf)
    }
    // 3.2. Databricks Mode
    else{
      sc = SparkContext.getOrCreate()
    }

    // 4. We configure the Spark variable
    var spark : SparkSession = SparkSession.builder().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n");
    }

    // 4. We call to myMain
    myMain(spark, myDatasetDir)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

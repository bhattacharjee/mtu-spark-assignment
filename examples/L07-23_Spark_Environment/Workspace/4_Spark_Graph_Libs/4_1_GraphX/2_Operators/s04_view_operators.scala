
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION getVertices
  // ------------------------------------------
  def getVertices(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation A1: We get the VertexRDD
    val vertexRDD = myGraphRDD.vertices

    // 3. Operation A2: We collect the RDD
    val resVAL = vertexRDD.collect()

    // 4. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // FUNCTION getEdges
  // ------------------------------------------
  def getEdges(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation A1: We get the EdgeRDD
    val edgesRDD = myGraphRDD.edges

    // 3. Operation A2: We collect the RDD
    val resVAL = edgesRDD.collect()

    // 4. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }


  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => getVertices(sc, myDatasetDir)
      case 2  => getEdges(sc, myDatasetDir)
    }
  }


  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option = 2

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

    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n")
    }

    // 5. We call to myMain
    myMain(sc, myDatasetDir, option)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

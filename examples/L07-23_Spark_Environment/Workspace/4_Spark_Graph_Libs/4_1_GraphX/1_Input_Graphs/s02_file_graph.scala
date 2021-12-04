
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

  //-------------------------------------
  // FUNCTION myMain
  //-------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. We persist myGraph
    myGraphRDD.persist()

    // 3. Operation A1: We get the num of vertices
    val resVAL1 = myGraphRDD.numVertices
    printf("%d\n", resVAL1)

    // 4. Operation A2: We get the num of edges
    val resVAL2 = myGraphRDD.numEdges
    printf("%d\n", resVAL2)
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

    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n")
    }

    // 4. We call to myMain
    myMain(sc, myDatasetDir)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

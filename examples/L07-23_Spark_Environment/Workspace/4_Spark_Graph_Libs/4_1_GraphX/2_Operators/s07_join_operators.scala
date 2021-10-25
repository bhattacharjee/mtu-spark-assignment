
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
  // FUNCTION myJoinVerticesLambda
  // ------------------------------------------
  def myJoinVerticesLambda(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation C2: We create an RDD of VertexID
    val myVertexRDD = sc.parallelize(Array((1L, 10), (2L, 10), (3L, 10), (20L, 10)))

    // 3. Operation T1: We join myGraphRDD with myVertexRDD
    val newGraphRDD = myGraphRDD.joinVertices(myVertexRDD)((id, oldCost, extraCost) => oldCost + extraCost)

    // 4. Operation A1: We get the verticesRDD
    val newVerticesRDD = newGraphRDD.vertices

    // 5. Operation A2: We collect verticesRDD
    val resVAL = newVerticesRDD.collect()

    // 6. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myJoinFunction
  // ------------------------------------------
  val myJoinFunction: (VertexId, Int, Int) => Int = (vId: VertexId, vVal: Int, rDDVal: Int) => {
    // 1. We create the output variable
    var res: Int = vVal + rDDVal

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION myJoinVertices
  // ------------------------------------------
  def myJoinVertices(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation C2: We create an RDD of VertexID
    val myVertexRDD = sc.parallelize(Array((1L, 10), (2L, 10), (3L, 10), (20L, 10)))

    // 3. Operation T1: We join myGraphRDD with myVertexRDD
    val newGraphRDD = myGraphRDD.joinVertices(myVertexRDD)(myJoinFunction)

    // 4. Operation A1: We get the verticesRDD
    val newVerticesRDD = newGraphRDD.vertices

    // 5. Operation A2: We collect verticesRDD
    val resVAL = newVerticesRDD.collect()

    // 6. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => myJoinVerticesLambda(sc, myDatasetDir)
      case 2  => myJoinVertices(sc, myDatasetDir)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option = 1

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


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
  // FUNCTION myPregelLambda
  // ------------------------------------------
  def myPregelLambda(sc: SparkContext, myDatasetDir: String, num_iterations: Int): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We make the vertices of the graph to be equal to their vID
    val myNewGraphRDD = myGraphRDD.mapVertices( (vId, v) => vId.toInt )

    // 3. Operation T2: We run the iterative algorithm via the Pregel API
    val myFinalGraphRDD = myNewGraphRDD.pregel(0, num_iterations, EdgeDirection.Both)(
      (vId, value, initialMessage) => value + initialMessage, // Vertex Program
      triplet => {  // Send Message
        Iterator((triplet.dstId, triplet.srcAttr), (triplet.srcId, triplet.dstAttr))
      },
      (a, b) => a + b) // Merge Message

    // 4. Operation A1: We get the VertexRDD
    val vertexRDD = myFinalGraphRDD.vertices

    // 5. Operation A2: We collect the RDD
    val resVAL = vertexRDD.collect()

    // 6. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myVProg
  // ------------------------------------------
  val myVProg: (VertexId, Int, Int) => Int = (vId: VertexId, value: Int, initialMessage: Int) => {
    // 1. We create the output variable
    var res: Int = value + initialMessage

    // 2. We return res
    res
  }

  // ------------------------------------------
  // INLINE FUNCTION mySendMsg
  // ------------------------------------------
  val mySendMsg: EdgeTriplet[Int, Int] => Iterator[(VertexId, Int)] = (e : EdgeTriplet[Int, Int]) => {
    // 1. We send the message to both src and dst
    Iterator((e.dstId, e.srcAttr), (e.srcId, e.dstAttr))
  }

  // ------------------------------------------
  // INLINE FUNCTION myMergeMsg
  // ------------------------------------------
  val myMergeMsg: (Int, Int) => Int = (v1: Int, v2: Int) => {
    // 1. We create the output variable
    var res : Int = v1 + v2

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION myPregel
  // ------------------------------------------
  def myPregel(sc: SparkContext, myDatasetDir: String, num_iterations: Int): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We make the vertices of the graph to be equal to their vID
    val myNewGraphRDD = myGraphRDD.mapVertices( (vId, v) => vId.toInt )

    // 3. Operation T2: We run the iterative algorithm via the Pregel API
    val myFinalGraphRDD =
      myNewGraphRDD.pregel(0, num_iterations, EdgeDirection.Both)(myVProg, mySendMsg, myMergeMsg)

    // 4. Operation A1: We get the VertexRDD
    val vertexRDD = myFinalGraphRDD.vertices

    // 5. Operation A2: We collect the RDD
    val resVAL = vertexRDD.collect()

    // 6. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }



  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int, num_iterations: Int): Unit = {
    option match {
      case 1  => myPregelLambda(sc, myDatasetDir, num_iterations)
      case 2  => myPregel(sc, myDatasetDir, num_iterations)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val num_iterations = 3
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
    myMain(sc, myDatasetDir, option, num_iterations)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

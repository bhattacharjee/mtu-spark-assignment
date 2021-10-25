
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
  // INLINE FUNCTION mySendMsg
  // ------------------------------------------
  val mySendMsg: EdgeContext[Int, Int, Int] => Unit = (e : EdgeContext[Int, Int, Int]) => {
    // 1. We send the message to both src and dst
    e.sendToSrc(e.dstAttr)
    e.sendToDst(e.srcAttr)
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
  // FUNCTION myAggregateMessages
  // ------------------------------------------
  def myAggregateMessages(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We make the vertices of the graph to be equal to their vID
    val myNewGraphRDD = myGraphRDD.mapVertices( (vId, v) => vId.toInt )

    // 3. Operation T2: We aggregate messages for myGraphRDD
    val myVertexRDD = myNewGraphRDD.aggregateMessages[Int](mySendMsg, myMergeMsg)

    // 4. Operation A1: We collect verticesRDD
    val resVAL = myVertexRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myPrint
  // ------------------------------------------
  val myPrint: Array[VertexId] => Unit = (a : Array[VertexId]) => {
    // 1. We print all elements
    for (name <- a){
      println(name)
    }
    println()
  }

  // ------------------------------------------
  // FUNCTION myCollectNeighborIds
  // ------------------------------------------
  def myCollectNeighborIds(sc: SparkContext, myDatasetDir: String, direction: Boolean): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. We specify the direction we want to aggregate
    var dir : EdgeDirection = EdgeDirection.Out
    if (direction == false){
      dir = EdgeDirection.In
    }

    // 3. Operation T1: We collect the neighbour VertexId of each node. --- Very costly operation!!!
    val myVertexRDD = myGraphRDD.collectNeighborIds(dir)

    // 4. Operation A1: We collect verticesRDD
    val resVAL = myVertexRDD.collect()

    // 5. We print the result
    for( vertexInfo <- resVAL ){
      printf( "--------------\nVertexId = %d\n--------------\n", (vertexInfo._1).toInt )
      for (e <- vertexInfo._2){
        println(e)
      }
    }
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int, direction: Boolean): Unit = {
    option match {
      case 1  => myAggregateMessages(sc, myDatasetDir)
      case 2  => myCollectNeighborIds(sc, myDatasetDir, direction)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option = 2
    val direction = false

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
    myMain(sc, myDatasetDir, option, direction)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

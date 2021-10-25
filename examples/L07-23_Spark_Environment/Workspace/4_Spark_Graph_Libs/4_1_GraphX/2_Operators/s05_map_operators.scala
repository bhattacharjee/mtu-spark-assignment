
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
  // FUNCTION myMapVerticesLambda
  // ------------------------------------------
  def myMapVerticesLambda(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapVertices( (vId, v) => vId + v )

    // 3. Operation A1: We get the VertexRDD
    val vertexRDD = newGraphRDD.vertices

    // 4. Operation A2: We collect vertexRDD
    val resVAL = vertexRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myVertexFunction
  // ------------------------------------------
  val myVertexFunction: (VertexId, Int) => Int = (vId: VertexId, v: Int) => {
    // 1. We create the output variable
    var res : Int = vId.toInt + v

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION myMapVertices
  // ------------------------------------------
  def myMapVertices(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapVertices( myVertexFunction )

    // 3. Operation A1: We get the VertexRDD
    val vertexRDD = newGraphRDD.vertices

    // 4. Operation A2: We collect vertexRDD
    val resVAL = vertexRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // FUNCTION myMapEdgesLambda
  // ------------------------------------------
  def myMapEdgesLambda(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapEdges( e => e.attr + 1 )

    // 3. Operation A1: We get the EdgeRDD
    val edgesRDD = newGraphRDD.edges

    // 4. Operation A2: We collect edgesRDD
    val resVAL = edgesRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myEdgeFunction
  // ------------------------------------------
  val myEdgeFunction: Edge[Int] => Int = (e: Edge[Int]) => {
    // 1. We create the output variable
    var res : Int = e.attr + 1

    // 2. We return res
    res
  }


  // ------------------------------------------
  // FUNCTION myMapEdges
  // ------------------------------------------
  def myMapEdges(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapEdges( myEdgeFunction )

    // 3. Operation A1: We get the EdgeRDD
    val edgesRDD = newGraphRDD.edges

    // 4. Operation A2: We collect edgesRDD
    val resVAL = edgesRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }


  // ------------------------------------------
  // FUNCTION myMapTripletsLambda
  // ------------------------------------------
  def myMapTripletsLambda(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapTriplets( e => e.attr + 1 )

    // 3. Operation A1: We get the EdgeRDD
    val edgesRDD = newGraphRDD.edges

    // 4. Operation A2: We collect edgesRDD
    val resVAL = edgesRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myEdgeTripletFunction
  // ------------------------------------------
  val myEdgeTripletFunction: EdgeTriplet[Int, Int] => Int = (e: EdgeTriplet[Int, Int]) => {
    // 1. We create the output variable
    var res : Int = e.attr + 1

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION myMapTriplets
  // ------------------------------------------
  def myMapTriplets(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.mapTriplets( myEdgeTripletFunction )

    // 3. Operation A1: We get the EdgeRDD
    val edgesRDD = newGraphRDD.edges

    // 4. Operation A2: We collect edgesRDD
    val resVAL = edgesRDD.collect()

    // 5. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }


  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => myMapVerticesLambda(sc, myDatasetDir)
      case 2  => myMapVertices(sc, myDatasetDir)
      case 3  => myMapEdgesLambda(sc, myDatasetDir)
      case 4  => myMapEdges(sc, myDatasetDir)
      case 5  => myMapTripletsLambda(sc, myDatasetDir)
      case 6  => myMapTriplets(sc, myDatasetDir)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option = 3

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


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
  // FUNCTION myReverse
  // ------------------------------------------
  def myReverse(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We assign a new value to each vertex
    val newGraphRDD = myGraphRDD.reverse

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
  // INLINE FUNCTION myTripletsFunction
  // ------------------------------------------
  val myTripletsFunction: EdgeTriplet[Int, Int] => Boolean = (e: EdgeTriplet[Int, Int]) => {
    // 1. We create the output variable
    var res: Boolean = true

    // 2. If both vertex are even, we set it to false
    if ((e.srcId % 2) == (e.dstId % 2))
      res = false

    // 3. We return res
    res
  }


  // ------------------------------------------
  // INLINE FUNCTION myVertexFunction
  // ------------------------------------------
  val myVertexFunction: (VertexId, Int) => Boolean = (vId: VertexId, value: Int) => {
    // 1. We create the output variable
    var res: Boolean = true

    // 2. If the vertex is bigger than 3
    if (vId <= 3)
      res = false

    // 3. We return res
    res
  }


  // ------------------------------------------
  // FUNCTION mySubgraph
  // ------------------------------------------
  def mySubgraph(sc: SparkContext, myDatasetDir: String): Unit = {
    // 1. We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 2. Operation T1: We filter the vertices and edges we are interested into
    val newGraphRDD = myGraphRDD.subgraph( myTripletsFunction, myVertexFunction )

    // 3. Operation P1: We persist newGraphRDD
    newGraphRDD.persist()

    // 4. Operation A1: We get the verticesRDD
    val verticesRDD = newGraphRDD.vertices

    // 5. Operation A2: We collect verticesRDD
    val resVAL1 = verticesRDD.collect()

    // 6. Operation A3: We get the EdgeRDD
    val edgesRDD = newGraphRDD.edges

    // 7. Operation A4: We collect edgesRDD
    val resVAL2 = edgesRDD.collect()

    // 8. We print the result
    for( item <- resVAL2 ){
      println(item)
    }
  }


  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, option: Int): Unit = {
    option match {
      case 1  => myReverse(sc, myDatasetDir)
      case 2  => mySubgraph(sc, myDatasetDir)
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

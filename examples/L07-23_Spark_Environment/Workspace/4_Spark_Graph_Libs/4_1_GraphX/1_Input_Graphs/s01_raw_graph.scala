
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  //-------------------------------------
  // FUNCTION myMain
  //-------------------------------------
  def myMain(sc: SparkContext): Unit = {
    // 1. Operation C1: We create myGraphRDD

    // 1.1. We create an RDD for the vertices
    val myVerticesRDD: RDD[(VertexId, Int)] =
      sc.parallelize(Array((1L, 1), (2L, 2), (3L, 3), (4L, 4), (5L, 5), (6L, 6), (7L, 7), (8L, 8), (9L, 9)))

    // 1.2. We create an RDD for edges
    val myEdgesRDD: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 4L, "1-4"), Edge(2L, 8L, "2-8"), Edge(3L, 6L, "3-6"), Edge(4L, 7L, "4-7"), Edge(5L, 2L, "5-2"), Edge(6L, 9L, "6-9"), Edge(7L, 1L, "7-1"), Edge(8L, 6L, "8-6"), Edge(8L, 5L, "8-5"), Edge(9L, 7L, "9-7"), Edge(9L, 3L, "9-3")))

    // 1.3. We define a default vertex, as the Graph constructor requires it (in case there is an edge with a missing vertex)
    val defaultVertexVAL = 0

    // 1.4. We create our graph
    val myGraphRDD = Graph(myVerticesRDD, myEdgesRDD, defaultVertexVAL)

    // 2. Operation P1: We persist myGraphRDD
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

    // 2. We configure the Spark Context sc
    var sc : SparkContext = null;

    // 2.1. Local mode
    if (localFalseDatabricksTrue == false){
      // 2.1.1. We create the configuration object
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("MyProgram")

      // 1.2. We initialise the Spark Context under such configuration
      sc = new SparkContext(conf)
    }
    else{
      sc = SparkContext.getOrCreate()
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n");
    }

    // 3. We call to myMain
    myMain(sc)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

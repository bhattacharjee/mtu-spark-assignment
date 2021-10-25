
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.graphframes.lib.AggregateMessages
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.graphframes.GraphFrame

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION motifFindingStateless
  // ------------------------------------------
  def motifFindingStateless(spark: SparkSession, myDatasetDir: String, query: Int): Unit = {
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
    val myVerticesDF = myEdgesDF.select("src")
      .withColumnRenamed("src", "id")
      .dropDuplicates()
      .withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation P2: We persist myGF
    myGF.persist()

    // 3. Next, we perform some queries, showing the resultDF

    if (query == 1) {
      // 3.1.1 Operation T4: A cycle among 3 vertices
      val myResultDF1 = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")

      // 3.1.2: Operation A1: We show the solution
      myResultDF1.show()
    }

    if (query == 2) {
      // 3.2.1 Operation T5: A transitivity relation with no cycle
      val myResultDF2 = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); !(c)-[]->(a); !(a)-[]->(c)")

      // 3.2.2: Operation A2: We show the solution
      myResultDF2.show()
    }

    if (query == 3) {
      // 3.3.1 Operation T6: Vertices c with in-degree at least two and out-degree at least one
      //                     Vertices d with out-degree at least two and in-degree at least one
      //                     With a connection from c to d, but not from d to c

      val find_c_auxDF = myGF.find("(a)-[e1]->(c); (b)-[e2]->(c)")
      val find_c_aux2DF = find_c_auxDF.filter("a != b")
      val find_c_DF = find_c_aux2DF.dropDuplicates("c").withColumnRenamed("c", "c1")

      val find_d_auxDF = myGF.find("(d)-[e3]->(e); (d)-[e4]->(f)")
      val find_d_aux2DF = find_d_auxDF.filter("e != f")
      val find_d_DF = find_d_aux2DF.dropDuplicates("d").withColumnRenamed("d", "d1")

      val find_connections_DF = myGF.find("(c)-[e5]->(d); !(d)-[]->(c)")

      val join1_DF = find_connections_DF.join(find_c_DF, col("c") === col("c1"), "inner")
      val join2_DF = join1_DF.join(find_d_DF, col("d") === col("d1"), "inner")

      val myResultDF3 = join2_DF.select("c", "d")

      // 3.3.2: Operation A2: We show the solution
      myResultDF3.show()
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myStateUpdate
  // ------------------------------------------
  val myStateUpdate: (Column, Column) => Column = (accum, vertexId) => {
    // 1. We create the output variable
    var res: Column = when(vertexId % 2 === 1, accum + 1).otherwise(accum)

    // 2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION motifFindingStateful
  // ------------------------------------------
  def motifFindingStateful(spark: SparkSession, myDatasetDir: String): Unit = {
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
    val myVerticesDF = myEdgesDF.select("src")
      .withColumnRenamed("src", "id")
      .dropDuplicates()
      .withColumn("value", lit(1))

    // 1.6. Operation C2: We create myGF from my_verticesDF and my_edgesDF
    val myGF = GraphFrame(myVerticesDF, myEdgesDF)

    // 2. Operation P2: We persist myGF
    myGF.persist()

    // 3. We perform our stateful query

    // 3.1. Operation T4: We find our pattern
    val myPatternDF = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")

    // 3.2. We define our state_update function, to be applied for each element in the motif
    // Inline myStateUpdate function defined above

    // 3.3. We define our state_computation to the elements of the sequence
    val my_state_generate = { Seq("a", "b", "c")
      .foldLeft(lit(0))((accum, v) => myStateUpdate(accum, col(v)("id"))) }

    // 3.4. Operation T5: We apply our state_computation function to my_patternDF
    val myResultDF = myPatternDF.where(my_state_generate >= 2)

    // 3.5. Operation A1: We show my_resultDF
    myResultDF.show()
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(spark: SparkSession, myDatasetDir: String, option: Int, query : Int): Unit = {
    option match {
      case 1  => motifFindingStateless(spark, myDatasetDir, query)
      case 2  => motifFindingStateful(spark, myDatasetDir)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val option : Integer = 2
    val query : Integer = 3

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
    myMain(spark, myDatasetDir, option, query)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

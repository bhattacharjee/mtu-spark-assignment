
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.graphframes._
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(spark: SparkSession): Unit = {
        // 1. Operation C1: We create myVerticesDF
        val myVerticesDF = spark.createDataFrame(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9))).toDF("id", "value")

        // 2. Operation C2: We create my_edgesDF
        val myEdgesDF = spark.createDataFrame(Array((1, 4, "1-4"), (2, 8, "2-8"), (3, 6, "3-6"), (4, 7, "4-7"), (5, 2, "5-2"), (6, 9, "6-9"), (7, 1, "7-1"), (8, 6, "8-6"), (8, 5, "8-5"), (9, 7, "9-7"), (9, 3, "9-3"))).toDF("src", "dst", "value")

        // 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
        val myGF = GraphFrame(myVerticesDF, myEdgesDF)

        // 4. Operation P1: We persist myGF
        myGF.persist()

        // 5. Operation T1: We revert back to sol_verticesDF
        val solVerticesDF = myGF.vertices

        // 6. Operation A1: We display sol_verticesDF
        solVerticesDF.show()

        // 7. Operation T2: We revert back to sol_edgesDF
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

        // 3. We configure the Spark variable
        var spark : SparkSession = SparkSession.builder().getOrCreate()
        Logger.getRootLogger.setLevel(Level.WARN)
        for( index <- 1 to 10){
            printf("\n");
        }

        // 4. We call to myMain
        myMain(spark)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

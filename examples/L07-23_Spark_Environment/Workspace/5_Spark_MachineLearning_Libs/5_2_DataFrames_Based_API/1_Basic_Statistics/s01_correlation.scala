
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, spark: SparkSession, myDatasetDir: String, correlationMethod: String): Unit = {

    // 1.1. We define the Schema of our DF.
    val mySchema = StructType(List(StructField("Col1", IntegerType, true), StructField("Col2", IntegerType, true)))

    // 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    val inputDF = spark.read.format("csv").option("delimiter", ";").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetDir)

    // 2. Operation T1: We fill in the empty fields
    val noNullDF = inputDF.na.fill(0)

    // 3. We reformat the columns

    // 3.1. We create the Vector Assembler
    val assembler = (new VectorAssembler()
      .setInputCols(Array("Col1", "Col2"))
      .setOutputCol("features"))

    // 3.2. Operation T2: We create the intermediate DF
    val featuresDF = assembler.transform(noNullDF).drop("Col1", "Col2")

    // 4. Operation T3: We compute pearson_resultDF
    val correlationResultDF = Correlation.corr(featuresDF, "features", correlationMethod)

    // 5. Operation A1: We take the result from it
    val correlationMatrix = correlationResultDF.head()
    println(correlationMatrix)
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    //val correlationMethod : String = "pearson"
    val correlationMethod : String = "spearman"

    //val datasetFileName : String = "pearson_2_vars_dataset.csv"
    //val datasetFileName : String = "pearson_3_vars_dataset.csv"
    val datasetFileName : String = "spearman_2_vars_dataset.csv"

    // 2. Local or Databricks
    val localFalseDatabricksTrue = true

    // 3. We set the path to my_dataset and my_result
    val myLocalPath = "../../"
    val myDatabricksPath = "/"

    var myDatasetDir : String = "FileStore/tables/5_Spark_MachineLearning_Libs/1_Basic_Statistics/" + datasetFileName

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


    // 3. We configure the Spark variable
    var spark : SparkSession = SparkSession.builder().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n")
    }

    // 5. We call to myMain
    myMain(sc, spark, myDatasetDir, correlationMethod)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

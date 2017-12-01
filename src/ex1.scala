import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ex1 extends App
{
  val conf = new SparkConf().setAppName("Exo1").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def jsonCreaturesToSpells(): Unit = {
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame = sparkSession.read.json("creatures.json").rdd

    val list = dataFrame.flatMap(
      row => row.getSeq[String](1).map()
    )

  }

  val sparkSession = new org.apache.spark.sql.SQLContext(sc)

  val df = sqlContext.read.json("creatures.json").rdd

  val stringRDD = df

  sqlContext.jsonRDD(stringRDD).registerTempTable("testjson")

  val dataframe = sqlContext.read.json()
}

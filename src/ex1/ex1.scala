package ex1

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ex1 extends App
{
  def jsonCreaturesToSpells(): Unit = {
    val conf = new SparkConf().setAppName("Exo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val dataFrame = sparkSession.read.json("creatures.json").rdd

    val list = dataFrame.flatMap(
      row => row.getSeq[String](1)
                .map( part => (part, row.getString(0)) )
    )

    val reducedByKey = list.reduceByKey(_+ " | " +_)

    reducedByKey.collect.foreach(println)

    reducedByKey.saveAsTextFile("spells")

  }

  override def main(args: Array[String]): Unit = {
    jsonCreaturesToSpells()
  }

}

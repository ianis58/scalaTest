package ex2

import ex2.combat.Combat1
import ex2.models.Creature
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ex2combat1 extends App
{
  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ex2: Premier combat").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    var creatures = new mutable.HashMap[VertexId, Creature]()

    for(i <- 1 to 14){
      var race = ""
      var hp = 0
      var hitMelee = 0
      var hitRanged = 0
      var shield = 0
      var speed = 0
      var dist = 0
      var alive = true

      if(i == 1){                 race = "angel_solar";         hp = 363; hitMelee = 35;  hitRanged = 31; shield = 44;  speed = 50; dist = 0    }
      else if(i >= 2 && i <= 10){ race = "orc_worg_rider";      hp = 13;  hitMelee = 6;   hitRanged = 4;  shield = 18;  speed = 20; dist = 110  }
      else if(i >= 11 && i <=13){ race = "orc_double_axe_fury"; hp = 142; hitMelee = 19;  hitRanged = 16; shield = 17;  speed = 40; dist = 150  }
      else if(i == 14){           race = "warlord";             hp = 141; hitMelee = 23;  hitRanged = 19; shield = 27;  speed = 30; dist = 170  }

      creatures.+=((i : VertexId, Creature(race, hp, i, hitMelee, hitRanged, dist, shield, speed, alive)))
    }

    val creaturesVerticesRDD = sc.makeRDD(creatures.toArray)

    val firstEdge = 1
    val lastEdge = 14
    var y = 2
    var creaturesEdges = Array(Edge(1,2,"1"))

    for(x <- firstEdge+2 to lastEdge){
      creaturesEdges = creaturesEdges :+ Edge(firstEdge,x,s"$y")
      y+=1
    }

    val creaturesEdgesRDD = sc.makeRDD(creaturesEdges)

    val fightGraph = Graph.apply(creaturesVerticesRDD, creaturesEdgesRDD)
    val maxIterations = 200

    new Combat1().execute(fightGraph, maxIterations, sc)
  }
}

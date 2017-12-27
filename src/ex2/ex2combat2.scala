package ex2

import ex2.combat.Combat2
import ex2.models.Creature
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ex2combat2 extends App
{
  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ex2: Deuxième combat").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    var creatures = new mutable.HashMap[VertexId, Creature]()

    for(i <- 1 to 220){
      var race = ""
      var hp = 0
      var hitMelee = 0
      var hitRanged = 0
      var shield = 0
      var speed = 0
      var dist = 0
      var alive = true

      if(i == 1){                   race = "angel_solar";             hp = 363; hitMelee = 35;  hitRanged = 31; shield = 44;  speed = 50; dist = 40  }
      else if(i >= 2 && i <= 3){    race = "angel_planetar";          hp = 229; hitMelee = 27;  hitRanged = 0;  shield = 32;  speed = 30; dist = 33  }
      else if(i >= 4 && i <= 5){    race = "angel_movanic_deva";      hp = 126; hitMelee = 17;  hitRanged = 0;  shield = 24;  speed = 40; dist = 25  }
      else if(i >= 6 && i <= 10){   race = "angel_astral_deva";       hp = 172; hitMelee = 26;  hitRanged = 0;  shield = 29;  speed = 50; dist = 27  }
      else if(i == 11){             race = "green_dragon_great_wyrm"; hp = 391; hitMelee = 33;  hitRanged = 0;  shield = 37;  speed = 40; dist = 30  }
      else if(i >= 12 && i <= 21){  race = "angel_slayer";            hp = 112; hitMelee = 21;  hitRanged = 19; shield = 26;  speed = 40; dist = 110 }
      else if(i >= 22 && i <= 220){ race = "orc_barbarian";           hp = 42;  hitMelee = 11;  hitRanged = 5;  shield = 15;  speed = 30; dist = 110 }

      creatures.+=((i : VertexId, Creature(race, hp, i, hitMelee, hitRanged, dist, shield, speed, alive)))
    }

    val creaturesVerticesRDD = sc.makeRDD(creatures.toArray)

    val firstEdge = 12
    val lastEdge = 220
    var y = 2
    var creaturesEdges = Array(Edge(1,11,"1"))
    val random  = scala.util.Random

    for(x <- firstEdge to lastEdge){
      val c = random.nextInt(10)+1 //selection aleatoire de l'ennemi

      creaturesEdges = creaturesEdges :+ Edge(c,x,s"$y")

      y+=1
    }

    val creaturesEdgesRDD = sc.makeRDD(creaturesEdges)

    val fightGraph = Graph.apply(creaturesVerticesRDD, creaturesEdgesRDD)
    val maxIterations = 200

    new Combat2().execute(fightGraph, maxIterations, sc)
  }
}

package ex2.combat1

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

case class Creature(var race: String,
               var hp: Int = 20,
               var id: VertexId = 1L,
               var hitMelee: Int = 10,
               var hitRanged: Int = 10,
               var dist: Int = 0,
               var shield: Int = 0,
               var speed: Int = 0,
               var alive : Boolean = true){

  override def toString: String = {
    if(alive){ print(Console.GREEN) } else { print(Console.RED) }
    if(dist < 100){
      s"vertexId : $id\t| hp : $hp\t| dist : $dist\t\t| alive : $alive\t| race : $race"
    }
    else{
      s"vertexId : $id\t| hp : $hp\t| dist : $dist\t| alive : $alive\t| race : $race"
    }
  }
}

object FightGraphUtils extends Serializable{

  var damage = 0

  def printGraph[VD, ED](g: Graph[VD, ED]): Unit = { //afficher le graph
    g.vertices.collect.foreach( vv => {
      println(s"${vv._2}")
      print(Console.WHITE)
    })
    println(" ")
  }

  def getNearestOrcId(g: Graph[Creature, String]): VertexId = { //id orc le + proche d'angel_solar
    val vertices = g.vertices.collect()
    var nearestOrc : VertexId = 0
    var distMax = 300
    for (i <- vertices) {
      if (i._2.dist < distMax && i._2.dist < 110  && i._2.dist != 0 && i._2.alive){
        distMax = i._2.dist
        nearestOrc = i._2.id
      }
      if (i._2.dist < distMax && i._2.dist >= 110 && i._2.dist != 0 && i._2.alive){
        distMax = i._2.dist
        nearestOrc = i._2.id
      }
    }
    nearestOrc
  }

  def getOrcsSumDamages(g: Graph[Creature, String]) { //somme dégats attaques orcs
    val vertices = g.vertices.collect()
    damage = 0
    var angelShield = 0
    //getting angel_solar shield value
    for (i <- vertices) {
      if (i._2.id == 1){
        angelShield = i._2.shield
      }
    }

    //orcs damage sum
    for (i <- vertices) {
      if (i._2.id != 1){
        if(i._2.alive){
          //if orc reachable with sword
          if(i._2.hitMelee > angelShield && i._2.dist <= 10){
            damage += i._2.hitMelee - angelShield
          }
          //if orc reachable with bow
          if(i._2.hitRanged > angelShield && i._2.dist > 10 && i._2.dist <= 110){
            damage += i._2.hitRanged - angelShield
          }
        }
      }
    }
  }
}

class Combat extends Serializable {
  def sendNodes(ctx: EdgeContext[Creature, String, Creature]) : Unit = {
    if(ctx.srcAttr.alive && ctx.dstAttr.alive) { //si les 2 créatures sont en vie on envoie le message
      ctx.sendToDst(ctx.srcAttr)
      ctx.sendToSrc(ctx.dstAttr)
    }
  }

  def selectNodeSource(source: Creature, dest : Creature): Creature = { //selection de la créature source
    if(source.id == 1)
      source
    else
      dest
  }

  def getCloser(creature : Creature): Int ={ //fonction pour faire avancer les créatures
    var newDist = creature.dist
    System.out.println("getCloser called on " + creature.toString)
    if(newDist > 10){
      if(creature.dist - creature.speed <= 5){
        newDist = 5
      }else{
        newDist -= creature.speed
      }
    }
    newDist
  }

  //gestion du combat, mécanique de jeu
  def action(sommet : Creature, target : VertexId, source : Creature) : Creature =
  {
    var c = Creature(sommet.race, sommet.hp, sommet.id, sommet.hitMelee, sommet.hitRanged, sommet.dist, sommet.shield, sommet.speed, true)

    if(source.race == "angel_solar"){ //si la créature source est angel_solar
      if(sommet.id == target){ //si la créature sommet est la target
        if(sommet.dist <= 10){
          if(sommet.hp - source.hitMelee <= 0){
            c.alive = false
            c.hp = 0
          }
          else{
            c.hp = sommet.hp - source.hitMelee
            c.dist = getCloser(sommet)
          }
        }
        else if(sommet.dist > 10 && sommet.dist <= 110){ //si orc est assez proche pour attaque arc
          if(sommet.hp - source.hitRanged <= 0){
            c.hp = 0
            c.alive = false
          }
          else{
            c.hp = sommet.hp - source.hitRanged
            c.dist = getCloser(sommet)
          }
        }
        else{
          c.dist = getCloser(sommet)
        }
      }

      else{
        c.dist = getCloser(sommet)
      }
      // remove some hp to angel_solar
    }
    else{
      if(sommet.hp - FightGraphUtils.damage <= 0){
        c.hp = 0
        c.dist = 0
        c.alive = false
      }
      else{
        c.hp = sommet.hp - FightGraphUtils.damage
      }
    }
    c
  }

  def execute(g: Graph[Creature, String], maxIterations: Int, sc : SparkContext): Graph[Creature, String] = { //fonction d'exécution
    var fightGraph = g
    var counter = 0
    var target: VertexId = 0

    val fields = new TripletFields(true, true, false)

    var keepFighting = true

    do {
      counter += 1

      println("________________________________________ROUND " + counter + "________________________________________")

      target = FightGraphUtils.getNearestOrcId(fightGraph)
      FightGraphUtils.getOrcsSumDamages(fightGraph)

      if (counter == maxIterations){
        keepFighting = false
      }

      //chaque vertice contient son message
      val verticesAndMessages = fightGraph.aggregateMessages[Creature](sendNodes, selectNodeSource, fields)

      //condition de terminaison
      if (verticesAndMessages.isEmpty()){
        keepFighting = false
      }

      //join des résultats
      fightGraph = fightGraph.joinVertices(verticesAndMessages)((vid, sommet, source) => action(sommet, target, source))
      FightGraphUtils.printGraph(fightGraph)

    } while (keepFighting)

    fightGraph
  }

}


object ex2 extends App
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

    new Combat().execute(fightGraph, maxIterations, sc)
  }
}

package ex2.utils

import ex2.models.Creature
import org.apache.spark.graphx.{Graph, VertexId}

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
package ex2.combat

import ex2.models.Creature
import ex2.utils.FightGraphUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeContext, Graph, TripletFields, VertexId}

abstract class Combat extends Serializable {
  def sendNodes(ctx: EdgeContext[Creature, String, Creature]): Unit = {
    if (ctx.srcAttr.alive && ctx.dstAttr.alive) { //si les 2 créatures sont en vie on envoie le message
      ctx.sendToDst(ctx.srcAttr)
      ctx.sendToSrc(ctx.dstAttr)
    }
  }

  def selectNodeSource(source: Creature, dest: Creature): Creature = { //selection de la créature source
    if (source.id == 1)
      source
    else
      dest
  }

  def getCloser(creature: Creature): Int = { //fonction pour faire avancer les créatures
    var newDist = creature.dist

    if (newDist > 10) {
      if (creature.dist - creature.speed <= 5) {
        newDist = 5
      } else {
        newDist -= creature.speed
      }
    }
    newDist
  }

  //gestion du combat, mécanique de jeu
  def action(sommet: Creature, target: VertexId, source: Creature): Creature

  def execute(g: Graph[Creature, String], maxIterations: Int, sc: SparkContext): Graph[Creature, String] = { //fonction d'exécution
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

      if (counter == maxIterations) {
        keepFighting = false
      }

      //chaque vertice contient son message
      val verticesAndMessages = fightGraph.aggregateMessages[Creature](sendNodes, selectNodeSource, fields)

      //condition de terminaison
      if (verticesAndMessages.isEmpty()) {
        keepFighting = false
      }

      //join des résultats
      fightGraph = fightGraph.joinVertices(verticesAndMessages)((vid, sommet, source) => action(sommet, target, source))
      FightGraphUtils.printGraph(fightGraph)

    } while (keepFighting)

    fightGraph
  }

}
package ex2.combat

import ex2.models.Creature
import ex2.utils.FightGraphUtils
import org.apache.spark.graphx.VertexId

class Combat1 extends Combat {
  //gestion du combat, mécanique de jeu
  override def action(sommet: Creature, target: VertexId, source: Creature) = {
    var c = Creature(sommet.race, sommet.hp, sommet.id, sommet.hitMelee, sommet.hitRanged, sommet.dist, sommet.shield, sommet.speed, true)

    if (source.race == "angel_solar") { //si la créature source est angel_solar
      if (sommet.id == target) { //si la créature sommet est la target
        if (sommet.dist <= 10) {
          if (sommet.hp - source.hitMelee <= 0) {
            c.alive = false
            c.hp = 0
          }
          else {
            c.hp = sommet.hp - source.hitMelee
            c.dist = getCloser(sommet)
          }
        }
        else if (sommet.dist > 10 && sommet.dist <= 110) { //si orc est assez proche pour attaque arc
          if (sommet.hp - source.hitRanged <= 0) {
            c.hp = 0
            c.alive = false
          }
          else {
            c.hp = sommet.hp - source.hitRanged
            c.dist = getCloser(sommet)
          }
        }
        else {
          c.dist = getCloser(sommet)
        }
      }

      else {
        c.dist = getCloser(sommet)
      }
      // remove some hp to angel_solar
    }
    else {
      if (sommet.hp - FightGraphUtils.damage <= 0) {
        c.hp = 0
        c.dist = 0
        c.alive = false
      }
      else {
        c.hp = sommet.hp - FightGraphUtils.damage
      }
    }
    c
  }
}

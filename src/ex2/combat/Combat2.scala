package ex2.combat

import ex2.models.Creature
import ex2.utils.FightGraphUtils
import org.apache.spark.graphx.VertexId

class Combat2 extends Combat {
  //gestion du combat, mécanique de jeu
  override def action(sommet: Creature, target: VertexId, source: Creature) = {
    var random = scala.util.Random
    val c = Creature(sommet.race, sommet.hp, sommet.id, sommet.hitMelee, sommet.hitRanged, sommet.dist, sommet.shield, sommet.speed, true)

    if(source.id >=1 && source.id <= 10){ //si la créature source est de la TEAM 1
      if(random.nextInt(10) == 5) { //une chance sur 10
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
    }
    else{// remove some hp to angel_solar
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
}

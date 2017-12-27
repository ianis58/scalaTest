package ex2.models

import org.apache.spark.graphx.VertexId

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

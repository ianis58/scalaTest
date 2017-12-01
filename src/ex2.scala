/*
 Edmond La Chance UQAC 2017
 Vincent Porta UQAC 2017

Graph Coloring Algorithms on Spark GraphX
Here we have two fast Pregel-style graph coloring algorithms
Second algorithm is called Fast Coloring 2 (FC1)
FC2 is massively parallel and message passing is optimized.
 With each iteration, the graph becomes more and more colored, and the number of messages
 that has to be shared decreases automatically.
 Message Passing is optimized to use only a single big integer, while the original algorithm, FC1 uses
 sorted vectors of (color, VertexID)
 Nodes automatically go up in color automatically with each iteration.
 To achieve randomness, FC2 generates random IDs for each vertex prior to a graph iteration.

FC1 is the original greedy algorithm. At each iteration, every graph nodes broadcasts its color to
the neighborhood. After merging color,VertexID pairs, we get a sorted vector of color,VertexID pair.
By having this vector, after each iteration, a given graph node will decide to keep its color, or grab the
best available color (by inspecting the neighborhood vector, an optimal decision can be made)

The drawback of this algorithm is the incredible number of messages that has to be sent ou. Since a given
graph edge sends messages both ways, that is a total of Edges*2 messages at every iteration.
New versions of FC1 achieve better message passing by intelligently removing stable nodes.

And of course, a random version of FC1 is achieved by generating and assigning random IDs before the algorithm
starts.


This is a much faster random coloring algorithm for massive parallel computation
It will probably generate worst solutions however.
Uses aggregateMessages and bidirectional edges
Tiebreaks colors using the lowestId of vertices to decide

Notes :
Plus ya de machines dans le cluster, plus le task size est petit
c'est normal, car chaque machine va devoir handler moins de messages

Driver program memory

Dans le gros programme, take prend 53 m, mapPartitions prend 11 sec
C'est un programme qui ne shuffle pas les données.
C'est un programme avec des messages très très lourds.

TODO : Modifier le programme pour ne pas avoir besoin d'envoyer des messages
a des sommets qui ont deja changé de couleur
todo : faire une version qui arrête sans accumulateur

TODO: systeme pour ameliorer les couleurs de FC2, en mode pas 100 % parallele
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeTriplet, Graph, _}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//Fast coloring selection
//fast coloring evolution

//1. Essayer une autre structure de données pour les messages (Réduire task size)
//2. Utiliser le Kyro Serializer pour tester(Task size = 6 mb)
//Le programme arrête automatiquement quand plus aucun message n'est envoyé

//Having this inside GraphColoring caused so many problems omg
//ids are randomized
class node(var color : Int = 1, var knighthood : Boolean = false, var id: VertexId = 1L) extends Serializable
{
  //Utile pour imprimer le graphe après
  override def toString: String = s"id : $id color : $color knighthood : $knighthood"
}

object Graphs extends Serializable{

  /* Utils */
  //Fonction template pour imprimer n'importe quel graphe
  def printg[VD, ED](g: Graph[VD, ED]): Unit = {
    g.vertices.collect.foreach( vv => {
      println(s"vertexID : ${vv._1} node: ${vv._2}")
    })
  }

  def getChromaticNumber(g: Graph[node, String]): Int = {
    val aa = g.vertices.collect()
    var maxColor = 0
    for (i <- aa) {
      if (i._2.color > maxColor) maxColor = i._2.color
    }
    maxColor
  }

  //Lire le graphe graphviz
  //  graph G {
  //    0 [label="p1 = 1 p2 = 1 p3 = 1 "];
  //    0 -- 1;
  def readGraphViz(filename: String, sc : SparkContext): Graph[node, String] = {
    import scala.io.Source
    var edgesVector: Vector[Edge[String]] = Vector.empty[Edge[String]]

    //Comment inferer le return??
    def treatLine(line: String): Option[Edge[String]] =
    {
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator

      //Ligne genre graph ou commentaire
      if (values(0)(0) > '9' || values(0)(0) < '0') {
        return Option(null)
      }

      //Gerer le label (pas besoin)
      if (values(1)(0) == '[' ) return Option(null)

      //Faire une edge
      val src = values(0).toLong + 1
      val dst = values(2).replace(";","").toLong + 1

      val e: Edge[String] = Edge(src.toLong, dst, line)
      return Option(e)

    }
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }
    val erdd = sc.makeRDD(edgesVector)
    val graph: Graph[node, String] = Graph.fromEdges(erdd, new node()) //set all vertices to the color 1, initially
    //NB_COULEUR_MAX = graph.vertices.count().toInt //Set Max number of colors to extreme upper bound
    return graph
  }

  //Lire le graphe
  def readGraphFromFile(filename: String, sc : SparkContext): Graph[node, String] = {
    import scala.io.Source
    var edgesVector: Vector[Edge[String]] = Vector.empty[Edge[String]]

    def getNumberVertex(): Int = {
      //lookup table
      //Aller chercher le nombre de nodes avant pour la lookup table
      for (line <- Source.fromFile(filename).getLines()) {
        //p edge 25 320
        //0  1   2  3
        if (line(0) == 'p') {
          val values = line.split(" ")
          return values(2).toInt
        }
      }
      0
    }

    var max = getNumberVertex()
    max += 1
    val lookup_table: Array[Array[Int]] = Array.fill(max, max)(5)

    //Comment inferer le return??
    def treatLine(line: String): Edge[String] =
    {
      //e 980 940
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator
    val src = values(1)
      val dest = values(2)

      //Si l'arete existe deja, on retourne une arete vide (Graphe non dirige svp!!)
      if (lookup_table(src.toInt)(dest.toInt) == 1) {
        val e: Edge[String] = Edge(0L, 0L, "error") //Return the 0,0 edge
        return e
      }

      //On ajoute dans la matrice d'adjacence
      lookup_table(src.toInt)(dest.toInt) = 1
      lookup_table(dest.toInt)(src.toInt) = 1

      val e: Edge[String] = Edge(src.toLong, dest.toLong, line)
      return e
    }
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      //Construire une liste de edges
      if (line(0) == 'e') {
        val edge = treatLine(line)
        //Check for errors
        if (!(edge.attr == "error")) {
          edgesVector = edgesVector :+ edge
        }
        else {
          //println("DUPLICATE EDGE : " + line)
        }
      }
    }
    val erdd = sc.makeRDD(edgesVector)
    val graph: Graph[node, String] = Graph.fromEdges(erdd, new node() ) //set all vertices to the color 1, initially
    //NB_COULEUR_MAX = graph.vertices.count().toInt //Set Max number of colors to extreme upper bound

    return graph
  }

}

//TODO : Generer les IDs aleatoirement et mettre ça dans le VertexData
//Comme ça, l'algorithme est maintenant random!

//This class implements the first coloring algorithm
//This algorithm sends vectors of colors,vertexId and each vertex chooses the best color by using this
//Todo : Implementer le système pour avoir une charge de messages qui diminue a chaque iteration

/**
  * FC2 algorithm
  * Message size optimized
  * Fastest algorithm
  */
class FC2 extends Serializable {

  //Var globales
  var NB_COULEUR_MAX = 0

  //(SEND MSG) This function lets nodes compete against each other by sending their VertexIds
  def sendIds(ctx: EdgeContext[node, String, VertexId]) : Unit = {

    //Do we send to a given vertex. SRC or DST.
    //To get a vertexid, a vertex must be "knighthood = false"
    //Also, vertices that are already knights cannot send VertexIDs
    if (ctx.srcAttr.knighthood == false && ctx.dstAttr.knighthood == false) {
      ctx.sendToDst( ctx.srcAttr.id  )
      ctx.sendToSrc( ctx.dstAttr.id)
    }
  }

  //Selects the best id (MergeMSG)
  def selectBestID(id1: VertexId, id2 : VertexId): VertexId = {
    if (id1 < id2) id1
    else id2
  }

  //Le sommet change de couleur avec ça
  def augmenterCouleur(vid : VertexId, sommet : node, bestId : VertexId) : node =
  {

    //If our ID is better, we keep this color and others will have to change
    if (sommet.id < bestId)
      return new node(sommet.color, true, sommet.id )
    else {
      return new node( sommet.color+1, false, sommet.id)
    }
  }

  //This is a helper function to execute aggregateMessages over a number of iterations on a certain graph
  //TODO : Dur a debugger a cause des actions
  def execute(g: Graph[node, String], maxIterations: Int, sc : SparkContext): Graph[node, String] = {
    var myGraph = g
    var counter = 0
    NB_COULEUR_MAX = g.vertices.count().toInt
    //Join strategy. Pas besoin des attributs de l'arête!!
    val fields = new TripletFields(true, true, false)
    println("NB COULEUR MAX = " + NB_COULEUR_MAX)

    def loop1: Unit = {
      while (true) {
        println("ITERATION NUMERO : " + (counter + 1))
        counter+=1
        if (counter == maxIterations) return
        //after this, each vertice contains its messages
        //TODO: optional triplet fields pour que aggregatemsg optimise son join strategy
        val vertice_and_messages = myGraph.aggregateMessages[VertexId](
          sendIds,
          selectBestID,
          fields //use an optimized join strategy (we don't need the edge attribute)
        )
        //Condition de terminaison https://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.api.java.JavaRDD
        if (vertice_and_messages.isEmpty()) return
        //Join les resultats des messages avec choisirCouleur
        myGraph = myGraph.joinVertices(vertice_and_messages)((vid, sommet, bestId) => augmenterCouleur(vid, sommet, bestId))
      }
    }
    loop1 //execute loop

    myGraph //return the result graph
  }

}


/**
  * Trouver la plus petite couleur disponible de 1 jusqu'au nombre chromatique du graph.
  * @param  sc SparkContext object
  **/
class Compute(sc : SparkContext) extends Serializable
{

  def randomize_ids(g : Graph[node, String]): Graph[node, String] =
  {
    val count : Int = g.vertices.count().toInt
    //Preparer un graph random
    //Randomizer les ids
    var ids_random : ArrayBuffer[Int] = new ArrayBuffer()
    //val seed = Random.nextInt(100)
    // println("le seed est : "+seed)
    // Random.setSeed(seed)
    Random.shuffle(1 to count).copyToBuffer(ids_random)
    var cc  = 0
    var vertices = g.vertices.collect()
    vertices = vertices.map(  v => {
      val n = new node(1, false, ids_random(cc))
      cc += 1
      (v._1, n)
    })

    Graph( sc.makeRDD(vertices), g.edges)
  }

  //Run a graph (already in memory)
  //We can make the graph run X times
  //Algorithms supported : FC1, FC2
  //We also randomize the IDS before
  def run_graph(g : Graph[node, String], times: Int = 1,  maxIterations : Int = 400, algorithm: String = "FC2") : Unit = {
    //Repeat this coloring

    for (i <- 1 to times) {

      println("**************************************")
      println("**************************************")
      println("**************************************")
      println("**************************************")
      println("**************************************")
      println("**************************************")
      println(s"Iteration $i\n")

      //Generate random graph
      var myGraph = randomize_ids(g)

      println("Graph initial : ")
      Graphs.printg(myGraph)
      val coloring = new FC2()
      println("ALGORITHME FC2")
      val res = coloring.execute(myGraph, maxIterations, sc)
      println("\nNombre de couleur trouvées: " +  Graphs.getChromaticNumber(res))
      println("\nGraph resultat : ")
      Graphs.printg(res)
      println("\n")
    }
  }

  //Max = max iterations
  def run_col_graph(filename: String, maxIterations : Int, algorithm: String = "FC2"): Unit = {

    var myGraph: Graph[node, String] = Graphs.readGraphFromFile(filename, sc)

    var coloring = new FC2()
    myGraph = coloring.execute(myGraph, maxIterations, sc)
    println("RESULTATS")
    Graphs.printg(myGraph)
    println("\n")
  }

  def run_graphviz(filename : String, maxIterations : Int, algorithm : String = "FC2") : Unit = {

    var myGraph = Graphs.readGraphViz(filename, sc)

    //Todo : Generer le string des edge avec un programme, automatiquement
    var coloring = new FC2()
    myGraph = coloring.execute(myGraph, maxIterations, sc)
    println("RESULTATS")
    Graphs.printg(myGraph)
    println("\n")
  }

  //Run a whole computation of graphviz files
  //Takes a list of files as parameter
  def runFolderGraphVizFC2( names : List[String], times : Int = 10, maxIterations : Int, outFilename : String) : Unit = {
    for (i <- names) {
      val filename = i
      val coloring = new FC2()
      var myGraph = Graphs.readGraphViz(filename, sc)
      //Run the randomized id graph a couple of times (10 is default)
      run_graph(myGraph, times, maxIterations, "FC2")
      //val nbCouleurs = coloring.getChromaticNumber(myGraph)
      //println(s"Pour le fichier $filename voici le nombre de couleurs : $nbCouleurs\n")
    }
  }
}


object testFolderCalculs extends App {

  val conf = new SparkConf()
    .setAppName("Graph Coloring Tests")
    .setMaster("local[*]") //For local tests
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  var truc = new Compute(sc)
  truc.runFolderGraphVizFC2( List("6,2.txt", "7,2.txt") , 10, 400, "out.txt")
}


object testPetersenGraph extends App {

  val conf = new SparkConf()
    .setAppName("Petersen Graph (10 nodes) test")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  var myVertices = sc.makeRDD(Array(
    (1L, new node(id = 1L)), //A
    (2L, new node(id = 2)), //B
    (3L, new node(id = 3)), //C
    (4L, new node(id = 4)), //D
    (5L, new node(id = 5)), //E
    (6L, new node(id = 6)), //F
    (7L, new node(id = 7)), //G
    (8L, new node(id = 8)), //H
    (9L, new node(id = 9)), //I
    (10L, new node(id = 10)))) //J

  //15 EDGES
  //Petersen graph
  //Voir image de référence
  var myEdges = sc.makeRDD(Array(
    Edge(1L, 2L, "1"), Edge(1L, 3L, "2"), Edge(1L, 6L, "3"),
    Edge(2L, 7L, "4"), Edge(2L, 8L, "5"),
    Edge(3L, 4L, "6"), Edge(3L, 9L, "7"),
    Edge(4L, 5L, "8"), Edge(4L, 8L, "9"),
    Edge(5L, 6L, "10"), Edge(5L, 7L, "11"),
    Edge(6L, 10L, "12"),
    Edge(7L, 9L, "13"),
    Edge(8L, 10L, "14"),
    Edge(9L, 10L, "15")
  ))

  //Todo : Generer le string des edge avec un programme, automatiquement
  var myGraph = Graph(myVertices, myEdges)
  var truc = new Compute(sc)
  truc.run_graph(myGraph, 8, 400, "FC2")
}

object testFromIntelliJ extends App {

  //Time "macro"
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  val conf = new SparkConf()
    .setAppName("Graph Coloring Tests")
    .setMaster("local[*]") //For local tests
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  var truc = new Compute(sc)

  val t = time {
    truc.run_col_graph("graphs/str.txt", 400)
  }
  println(t)

}

object testFromIntelliJKyro extends App {

  //Time "macro"
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  val conf = new SparkConf()
    .setAppName("Graph Coloring Greedy labeling")
    .setMaster("local[*]") //For local tests
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[FC2]))

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  var truc = new Compute(sc)

  var t = time {
    truc.run_col_graph("graphs/str.txt", 400)
  }
  println(t)

}

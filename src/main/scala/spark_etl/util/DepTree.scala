package spark_etl.util

import scala.annotation.tailrec

/**
  * Constructs dependency graph consisting of Vertices and Edges:
  * - Vertex has id and VertexType
  * - VertexType classification:
  *
  *              VertexType
  *                  |
  *      +-----------+----------+
  *      |                      |
  *    RootType              OtherType
  * +----+-------+       +-+-+--+----+
  * |    |       |       |   |       |
  * L  LCheck  ECheck    E   T    Dangling
  *
  * - Edge has source and target Vertices
  *
  * Process:
  * - go through all E|ECheck|T|TCheck|L
  * - add all Edges, by first looking up all the Vertices, then linking them. If Vertex doesn't exist, mark it as Dangling
  *
  * Note: Order of Vertices and Edges is preserved by the LinkedHashSet
  */
class DepTree(knownVertices : Seq[Vertex]) {
  private val vertices = collection.mutable.LinkedHashSet[Vertex](knownVertices:_*)
  private val edges = collection.mutable.LinkedHashSet.empty[Edge]
  private val allTypes = Set(E, T, L, Echeck, Tcheck, Dangling)
  private val nonDangling = allTypes - Dangling

  /**
    * Add Edge, fetching from either a list of known, or marking it as Dangling.
    */
  def addEdge(sourceId: String, target: Vertex): Unit = {
    val source = vertices
      .collectFirst { case v @ Vertex(id, _:OtherType) if sourceId == id => v }
      .getOrElse(Vertex(sourceId, Dangling))
    vertices += source  // add if dangling
    edges += Edge(source, target)
  }

  def dangling: Seq[Edge] =
    edges.filter(_.source.`type` == Dangling).toSeq

  /**
    * Walk down the graph from root objects, collect all encountered Vertices.
    */
  def rootfull(types: Set[VertexType] = nonDangling): Seq[Vertex] = {
    val vs = collect(vertices.collect { case v @ Vertex(_, _: RootType) => v }.toSeq)
    vs.filter(v => types.contains(v.`type`))
  }

  /**
    * All vertices -- rootfull
    */
  def rootless: Seq[Vertex] =
    (vertices -- rootfull()).toSeq

  def forType(`type`: VertexType): Seq[Vertex] =
    rootfull().filter(_.`type` == `type`)

  def asDot(name: String = "Lineage", fontSize: Int = 12): String = {
    val plottableVertices = rootfull(Set(E, T, L))
    val plottableEdges = edges.filter(e => plottableVertices.contains(e.source) && plottableVertices.contains(e.target))
    val edgeStrs = plottableEdges.collect {
      case Edge(srcV, targetV)  =>
        if (targetV.`type` == T)
          s"${srcV.id} -> ${targetV.id} [style=dotted]"
        else
          s"${srcV.id} -> ${targetV.id}"
    }
    val verticeStrs = plottableVertices.collect {
      case Vertex(id, E) => s"$id"
      case Vertex(id, T) => s"""$id [shape=component]"""
      case Vertex(id, L) => s"""$id [shape=cylinder]"""
    }
    val rankStrs = plottableVertices.groupBy(_.`type`).flatMap {
      case (E, vs) => Seq(s"""{ rank=same; ${vs.map(_.id).mkString(" ")} }""")
      case (L, vs) => Seq(s"""{ rank=same; ${vs.map(_.id).mkString(" ")} }""")
      case _ => Nil
    }
    s"""digraph $name {
       |  rankdir=LR
       |  node [fontsize=$fontSize]
       |
       |  # edges
       |  ${edgeStrs.mkString("\n  ")}
       |
       |  # vertices
       |  ${verticeStrs.mkString("\n  ")}
       |
       |  # ranks
       |  ${rankStrs.toList.sorted.mkString("\n  ")}
       |}""".stripMargin
  }

  @tailrec
  private def collect(
                       roots: Seq[Vertex],
                       types: Set[VertexType] = nonDangling,
                       soFar: Seq[Vertex] = Nil): Seq[Vertex] = {
    val sources = roots.flatMap(v => edges.collect {
      case e if e.target == v && types.contains(e.source.`type`) && ! soFar.contains(e.source) => e.source
    })
    val soFar2 = (soFar ++ roots).distinct
    if (sources.nonEmpty)
      collect(sources, types, soFar2)
    else
      soFar2
  }
}

case class Vertex(id: String, `type`: VertexType)
case class Edge(source: Vertex, target: Vertex)

sealed trait VertexType { def asStr: String }
sealed trait RootType extends VertexType
sealed trait OtherType extends VertexType
object E extends OtherType { override def asStr = "extract" }
object T extends OtherType { override def asStr = "transform" }
object L extends RootType  { override def asStr = "load" }
object Echeck extends RootType { override def asStr = "extract-check" }
object Tcheck extends RootType { override def asStr = "transform-check" }
object Dangling extends OtherType { override def asStr = "dangling" }

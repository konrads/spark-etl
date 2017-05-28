package spark_etl.util

class DepTree() {
  private val candidates: collection.mutable.Set[Edge] = collection.mutable.LinkedHashSet.empty
  private val actuals: collection.mutable.Set[Edge] = collection.mutable.LinkedHashSet.empty

  // mutations
  def addCandidate(edge: Edge): Unit = candidates.add(edge)

  def addActual(childId: String, parent: Vertice): Unit = {
    val newActual = candidates
      .collectFirst { case r @ Edge(Vertice(cId, _), p) if cId == childId && p == parent => r }
      .getOrElse(Edge(Vertice(childId, Dangling), parent))
    actuals.add(newActual)
  }

  // getters
  def dangling: Set[Edge] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    allRoots.flatMap(descendants).filter(r => r.child.`type` == Dangling)
  }

  def forChildType(`type`: ETLVertice): Set[Edge] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    val nonDangs = allRoots.flatMap(descendants).filter(r => r.child.`type` == `type`)
    // map to original order
    candidates.intersect(nonDangs).toSet
  }

  def forParentType(`type`: ETLVertice): Set[Edge] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    val nonDangs = allRoots.flatMap(descendants).filter(r => r.parent.`type` == `type`)
    // map to original order
    candidates.intersect(nonDangs).toSet
  }

  def asDot(name: String = "Lineage"): String = {
    val plottableEdgeTypes = Set(E, T, L)
    val plottableEdges = actuals.filter(e => plottableEdgeTypes.contains(e.child.`type`) && plottableEdgeTypes.contains(e.parent.`type`))
    val plottableVertices = plottableEdges.map(_.child) ++ actuals.map(_.parent)
    val edges = plottableEdges.collect {
      case Edge(srcV, targetV)  =>
        if (targetV.`type` == T)
          s"${srcV.id} -> ${targetV.id} [style=dotted]"
        else
          s"${srcV.id} -> ${targetV.id}"
    }
    val vertices = plottableVertices.collect {
      case Vertice(id, E) => s"$id"
      case Vertice(id, T) => s"""$id [shape=component]"""
      case Vertice(id, L) => s"""$id [shape=cylinder]"""
    }
    val ranks = plottableVertices.groupBy(_.`type`).flatMap {
      case (E, vs) => Seq(s"""{ rank=same; ${vs.map(_.id).mkString(" ")} }""")
      case (L, vs) => Seq(s"""{ rank=same; ${vs.map(_.id).mkString(" ")} }""")
      case _ => Nil
    }
    s"""digraph $name {
      |  rankdir=LR
      |
      |  # edges
      |  ${edges.toList.sorted.mkString("\n  ")}
      |
      |  # vertices
      |  ${vertices.toList.sorted.mkString("\n  ")}
      |
      |  # ranks
      |  ${ranks.toList.sorted.mkString("\n  ")}
      |}""".stripMargin
  }

  private def actualParents(`type`: ETLVertice): Set[Vertice] =
    actuals.collect { case Edge(_, p @ Vertice(_, t)) if `type` == t => p }.toSet

//  private def actualChildren(`type`: ETLNode): Set[Node] =
//    actuals.collect { case Rel(c @ Node(_, t), p) if `type` == t => c }.toSet

  private def descendants(parent: Vertice): Set[Edge] = {
    val rels = actuals.collect { case r @ Edge(_, p) if parent == p => r }
    val children = rels.map(_.child)
    rels.toSet ++ children.flatMap(descendants)
  }

//  override def toString(): String =
//    s"candidates:\n -${candidates.mkString("\n -")}\nactuals:\n +${actuals.mkString("\n +")}"
}

case class Vertice(id: String, `type`: ETLVertice)
case class Edge(child: Vertice, parent: Vertice)

sealed trait ETLVertice { def asStr: String }
object E extends ETLVertice { override def asStr = "extract" }
object Echeck extends ETLVertice { override def asStr = "extract-check" }
object T extends ETLVertice { override def asStr = "transform" }
object Tcheck extends ETLVertice { override def asStr = "transform-check" }
object L extends ETLVertice  { override def asStr = "load" }
object Dangling extends ETLVertice  { override def asStr = "dangling" }

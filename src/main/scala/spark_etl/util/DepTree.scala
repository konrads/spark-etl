package spark_etl.util

class DepTree() {
  private val candidates: collection.mutable.Set[Rel] = collection.mutable.LinkedHashSet.empty
  private val actuals: collection.mutable.Set[Rel] = collection.mutable.LinkedHashSet.empty

  // mutations
  def addCandidate(rel: Rel): Unit = candidates.add(rel)

  def addActual(childId: String, parent: Node): Unit = {
    val newActual = candidates
      .collectFirst { case r @ Rel(Node(cId, _), p) if cId == childId && p == parent => r }
      .getOrElse(Rel(Node(childId, Dangling), parent))
    actuals.add(newActual)
  }

  // getters
  def dangling: Set[Rel] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    allRoots.flatMap(descendants).filter(r => r.child.`type` == Dangling)
  }

  def forChildType(`type`: ETLNode): Set[Rel] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    val nonDangs = allRoots.flatMap(descendants).filter(r => r.child.`type` == `type`)
    // map to original order
    candidates.intersect(nonDangs).toSet
  }

  def forParentType(`type`: ETLNode): Set[Rel] = {
    val allRoots = actualParents(L) ++ actualParents(Tcheck) ++ actualParents(Echeck)
    val nonDangs = allRoots.flatMap(descendants).filter(r => r.parent.`type` == `type`)
    // map to original order
    candidates.intersect(nonDangs).toSet
  }

  private def actualParents(`type`: ETLNode): Set[Node] =
    actuals.collect { case Rel(_, p @ Node(_, t)) if `type` == t => p }.toSet

//  private def actualChildren(`type`: ETLNode): Set[Node] =
//    actuals.collect { case Rel(c @ Node(_, t), p) if `type` == t => c }.toSet

  private def descendants(parent: Node): Set[Rel] = {
    val rels = actuals.collect { case r @ Rel(_, p) if parent == p => r }
    val children = rels.map(_.child)
    rels.toSet ++ children.flatMap(descendants)
  }

//  override def toString(): String =
//    s"candidates:\n -${candidates.mkString("\n -")}\nactuals:\n +${actuals.mkString("\n +")}"
}

case class Node(id: String, `type`: ETLNode)
case class Rel(child: Node, parent: Node)

sealed trait ETLNode { def asStr: String }
object E extends ETLNode { override def asStr = "extract" }
object Echeck extends ETLNode { override def asStr = "extract-check" }
object T extends ETLNode { override def asStr = "transform" }
object Tcheck extends ETLNode { override def asStr = "transform-check" }
object L extends ETLNode  { override def asStr = "load" }
object Dangling extends ETLNode  { override def asStr = "dangling" }

package spark_etl.util

import org.scalatest.{FlatSpec, Inside, Matchers}

class DepTreeSpec extends FlatSpec with Matchers with Inside {
  "DepTree" should "validate simple tree with no unaccounter-fors" in {
    val tree = depTree()

    // add actual deps
    tree.addActual("e1", Vertice("t1", T))
    tree.addActual("e2", Vertice("t1", T))
    tree.addActual("t1", Vertice("t2", T))
    tree.addActual("e3", Vertice("t2", T))
    tree.addActual("t1", Vertice("l1", L))
    tree.addActual("t2", Vertice("l2", L))

    // validate
    tree.dangling shouldBe Set.empty
    tree.forParentType(L) shouldBe Set(
      Edge(Vertice("t1", T), Vertice("l1", L)),
      Edge(Vertice("t2", T), Vertice("l2", L))
    )
    tree.forParentType(T) shouldBe Set(
      Edge(Vertice("e1", E), Vertice("t1", T)),
      Edge(Vertice("e2", E), Vertice("t1", T)),
      Edge(Vertice("t1", T), Vertice("t2", T)),
      Edge(Vertice("e3", E), Vertice("t2", T))
    )
  }

  it should "find dangling" in {
    val tree = depTree()

    // add actual deps
    tree.addActual("e1", Vertice("t1", T))
    tree.addActual("__bogus_e__", Vertice("t1", T))
    tree.addActual("t1", Vertice("l1", L))

    // validate
    tree.dangling shouldBe Set(Edge(Vertice("__bogus_e__", Dangling), Vertice("t1", T)))
    tree.forParentType(T) shouldBe Set(Edge(Vertice("e1", E), Vertice("t1", T)))
    tree.forChildType(E) shouldBe Set(Edge(Vertice("e1", E), Vertice("t1", T)))
  }

  private def depTree() = {
    val tree = new DepTree()
    // add available deps
    tree.addCandidate(Edge(Vertice("t1", T), Vertice("l1", L)))
    tree.addCandidate(Edge(Vertice("t2", T), Vertice("l1", L)))
    tree.addCandidate(Edge(Vertice("t1", T), Vertice("l2", L)))
    tree.addCandidate(Edge(Vertice("t2", T), Vertice("l2", L)))
    tree.addCandidate(Edge(Vertice("e1", E), Vertice("t1", T)))
    tree.addCandidate(Edge(Vertice("e2", E), Vertice("t1", T)))
    tree.addCandidate(Edge(Vertice("e3", E), Vertice("t1", T)))
    tree.addCandidate(Edge(Vertice("t1", T), Vertice("t2", T)))
    tree.addCandidate(Edge(Vertice("e1", E), Vertice("t2", T)))
    tree.addCandidate(Edge(Vertice("e2", E), Vertice("t2", T)))
    tree.addCandidate(Edge(Vertice("e3", E), Vertice("t2", T)))
    tree
  }
}

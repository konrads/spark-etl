package spark_etl.util

import org.scalatest.{FlatSpec, Matchers, Inside}

class DepTreeSpec extends FlatSpec with Matchers with Inside {
  "DepTree" should "validate simple tree with no unaccounter-fors" in {
    val tree = depTree()

    // add actual deps
    tree.addActual("e1", Node("t1", T))
    tree.addActual("e2", Node("t1", T))
    tree.addActual("t1", Node("t2", T))
    tree.addActual("e3", Node("t2", T))
    tree.addActual("t1", Node("l1", L))
    tree.addActual("t2", Node("l2", L))

    // validate
    tree.dangling shouldBe Set.empty
    tree.forParentType(L) shouldBe Set(
      Rel(Node("t1", T), Node("l1", L)),
      Rel(Node("t2", T), Node("l2", L))
    )
    tree.forParentType(T) shouldBe Set(
      Rel(Node("e1", E), Node("t1", T)),
      Rel(Node("e2", E), Node("t1", T)),
      Rel(Node("t1", T), Node("t2", T)),
      Rel(Node("e3", E), Node("t2", T))
    )
  }

  it should "find dangling" in {
    val tree = depTree()

    // add actual deps
    tree.addActual("e1", Node("t1", T))
    tree.addActual("__bogus_e__", Node("t1", T))
    tree.addActual("t1", Node("l1", L))

    // validate
    tree.dangling shouldBe Set(Rel(Node("__bogus_e__", Dangling), Node("t1", T)))
    tree.forParentType(T) shouldBe Set(Rel(Node("e1", E), Node("t1", T)))
    tree.forChildType(E) shouldBe Set(Rel(Node("e1", E), Node("t1", T)))
  }

  private def depTree() = {
    val tree = new DepTree()
    // add available deps
    tree.addCandidate(Rel(Node("t1", T), Node("l1", L)))
    tree.addCandidate(Rel(Node("t2", T), Node("l1", L)))
    tree.addCandidate(Rel(Node("t1", T), Node("l2", L)))
    tree.addCandidate(Rel(Node("t2", T), Node("l2", L)))
    tree.addCandidate(Rel(Node("e1", E), Node("t1", T)))
    tree.addCandidate(Rel(Node("e2", E), Node("t1", T)))
    tree.addCandidate(Rel(Node("e3", E), Node("t1", T)))
    tree.addCandidate(Rel(Node("t1", T), Node("t2", T)))
    tree.addCandidate(Rel(Node("e1", E), Node("t2", T)))
    tree.addCandidate(Rel(Node("e2", E), Node("t2", T)))
    tree.addCandidate(Rel(Node("e3", E), Node("t2", T)))
    tree
  }
}

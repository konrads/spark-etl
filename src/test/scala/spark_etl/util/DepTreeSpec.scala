package spark_etl.util

import org.scalatest.{FlatSpec, Inside, Matchers}

class DepTreeSpec extends FlatSpec with Matchers with Inside {
  val initVertices = Seq(
    Vertex("l1", L),
    Vertex("l2", L),
    Vertex("t1", T),
    Vertex("t2", T),
    Vertex("e1", E),
    Vertex("e2", E),
    Vertex("e3", E)
  )

  "DepTree" should "validate simple tree with no dangling" in {
    val tree = new DepTree(initVertices)

    // add actual deps
    tree.addEdge("e1", Vertex("t1", T))
    tree.addEdge("e2", Vertex("t1", T))
    tree.addEdge("t1", Vertex("t2", T))
    tree.addEdge("e3", Vertex("t2", T))
    tree.addEdge("t1", Vertex("l1", L), true)
    tree.addEdge("t2", Vertex("l2", L), true)

    // validate
    tree.dangling shouldBe Nil
    tree.forType(L) shouldBe Seq(
      Vertex("l1", L),
      Vertex("l2", L)
    )
    tree.forType(T) shouldBe Seq(
      Vertex("t1", T),
      Vertex("t2", T)
    )
  }

  it should "find dangling" in {
    val tree = new DepTree(initVertices)

    // add actual deps
    tree.addEdge("e1", Vertex("t1", T))
    tree.addEdge("__bogus_e__", Vertex("t1", T))
    tree.addEdge("t1", Vertex("l1", L), true)

    // validate
    tree.dangling shouldBe Seq(Edge(Vertex("__bogus_e__", Dangling), Vertex("t1", T), false))

    tree.rootless shouldBe Seq(Vertex("t2", T), Vertex("e2", E), Vertex("e3", E), Vertex("__bogus_e__", Dangling))

    tree.forType(T) shouldBe Seq(Vertex("t1", T))
    tree.forType(E) shouldBe Seq(Vertex("e1", E))
  }
}

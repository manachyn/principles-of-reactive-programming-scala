package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("gen1") = forAll { h: H =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("minimum of 2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, insert(b, empty))
    findMin(h) == ord.min(a, b)
  }

  property("delete minimum of 1") = forAll { a: Int =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }

  property("minimum of melding") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == ord.min(findMin(h1), findMin(h2))
  }

  property("associative melding") = forAll { (h:H, i:H, j:H) =>
    val a = meld(meld(h, i), j)
    val b = meld(h, meld(i, j))
    toList(a) == toList(b)
  }

  property("sorted sequence after deleting minimum") = forAll { h: H =>
    val l = toList(h)
    l == l.sorted
  }

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(a, h)


  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  def toList(h: H): List[A] = h match {
    case h if isEmpty(h) => Nil
    case h => findMin(h) :: toList(deleteMin(h))
  }

}

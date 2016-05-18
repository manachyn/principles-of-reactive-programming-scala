/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case op => root ! op
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      unstashAll()
    }
    case op => stash()
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) => {
      if (newElem == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Insert(requester, id, newElem)
        } else {
          subtrees += Left -> context.actorOf(BinaryTreeNode.props(newElem, false))
          requester ! OperationFinished(id)
        }
      } else if (elem < newElem) {
        if (subtrees contains Right) {
          subtrees(Right) ! Insert(requester, id, newElem)
        } else {
          subtrees += Right -> context.actorOf(BinaryTreeNode.props(newElem, false))
          requester ! OperationFinished(id)
        }
      }
    }
    case Contains(requester, id, value) => {
      if (value == elem && !removed) {
        requester ! ContainsResult(id, true)
      } else if (value < elem && subtrees.contains(Left)) {
        subtrees(Left) ! Contains(requester, id, value)
      } else if (value > elem && subtrees.contains(Right)) {
        subtrees(Right) ! Contains(requester, id, value)
      } else {
        requester ! ContainsResult(id, false)
      }
    }
    case Remove(requester, id, value) => {
      if (value < elem && subtrees.contains(Left)) {
        subtrees(Left) ! Remove(requester, id, value)
      } else if (elem < value && subtrees.contains(Right)) {
        subtrees(Right) ! Remove(requester, id, value)
      } else {
        if (value == elem) {
          removed = true
        }
        val response: OperationFinished = OperationFinished(id)
        requester ! response
      }
    }
    case CopyTo(treeNode) => {
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
      } else {
        val subnodes: Set[ActorRef] = subtrees.values.toSet
        subnodes foreach {
          _ ! CopyTo(treeNode)
        }
        if (!removed) {
          treeNode ! Insert(self, 0, elem)
        }
        context.become(copying(subnodes, removed))
      }
    }
  }


  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case operationFinished: OperationFinished => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.unbecome
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }
    }
    case CopyFinished => {
      val remaining = expected - sender
      if (remaining.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.unbecome
      } else {
        context.become(copying(remaining, insertConfirmed))
      }
    }
  }


}

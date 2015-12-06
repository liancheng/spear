package scraper.trees

import scala.collection.Iterator.iterate
import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.scalacheck.Prop.{BooleanOperators, all}
import org.scalacheck.util.Pretty
import org.scalacheck.{Shrink, Arbitrary, Gen}
import org.scalatest.prop.Checkers

import scraper.LoggingFunSuite
import scraper.trees.TreeNodeSuite.Node
import scraper.types.TestUtils

class TreeNodeSuite extends LoggingFunSuite with TestUtils with Checkers {
  private def genNode: Gen[Node] = Gen.parameterized { param =>
    val size = param.size
    if (size < 2) {
      Node(1, Nil)
    } else {
      for {
        width <- Gen choose (1, math.min(size - 1, 8))
        childrenSizes = scraper.generators.randomPartition(param.rng, size - 1, width)
        children <- Gen.sequence(childrenSizes.map(Gen.resize(_, genNode)))
      } yield Node(1, children.asScala.toArray.toSeq)
    }
  }

  implicit val arbNode = Arbitrary(genNode)

  implicit val nodeShrink: Shrink[Node] = Shrink { input =>
    if (input.isLeaf) {
      Stream.empty
    } else {
      input.children.toStream :+ removeLeaf(input)
    }
  }

  private def removeLeaf(node: Node): Node = {
    var stop = false
    node transformUp {
      case n if !stop && !n.isLeaf =>
        stop = true
        Node(1, Nil)
    }
  }

  implicit def prettyNode(tree: Node): Pretty = Pretty {
    _ => "\n" + tree.prettyTree
  }

  test("transformDown") {
    check { tree: Node =>
      tree transformDown {
        case node @ Node(_, children) =>
          node.copy(value = children.map(_.value).sum)
      } forall {
        case Node(value, Nil)      => value == 0
        case Node(value, children) => value == children.size
      }
    }
  }

  test("transformUp") {
    check { tree: Node =>
      tree transformUp {
        case n @ Node(_, children) =>
          n.copy(value = children.map(_.value).sum)
      } forall {
        case Node(value, _) => value == 0
      }
    }
  }

  test("collect") {
    check { tree: Node =>
      val even = tree collect { case n if n.children.size % 2 == 0 => n }
      val odd = tree collect { case n if n.children.size % 2 == 1 => n }
      val nodes = tree collect { case n => n }

      all(
        "all nodes should be collected" |:
          (nodes.size == tree.size),

        "a node can't be both even and odd" |:
          (even intersect odd).isEmpty,

        "a node must be either even or odd" |:
          (even.size + odd.size == nodes.size),

        "even nodes should be even" |:
          (even forall (_.children.size % 2 == 0)),

        "odd nodes should be odd" |:
          (odd forall (_.children.size % 2 == 1))
      )
    }
  }

  test("forall") {
    check { tree: Node =>
      all(
        "the generator we are using only generates nodes with value 1" |:
          tree.forall(_.value == 1),

        "for trees that has more than 1 node, there must be non-leaf nodes" |:
          (tree.size > 1) ==> !tree.forall(_.isLeaf)
      )
    }
  }

  test("exists") {
    check { tree: Node =>
      all(
        "a tree must have leaf node(s)" |:
          tree.exists(_.isLeaf),

        "the generator we are using only generates nodes with value 1" |:
          !tree.exists(_.value == 2)
      )
    }
  }

  // This test case is obviously wrong is there is a `Node` having 4 children and one of its child
  // is not leaf, let's see if we can shrink the input into the minimal format that can fail test.
  test("wrong exists") {
    check { tree: Node =>
      tree.exists(_.children.length == 4) == tree.wrongExists(_.children.length == 4)
    }
  }

  test("size") {
    check { tree: Node =>
      tree.size == (tree collect { case n => n.value }).sum
    }
  }

  test("depth") {
    check { tree: Node =>
      // Computes the depth by iterating over the tree and removing all the leaf nodes during each
      // iteration until only the root node is left.
      val iterations = iterate(tree) {
        _ transformDown {
          case n => n.copy(children = n.children filterNot (_.isLeaf))
        }
      } takeWhile (_.size > 1)

      tree.depth == iterations.size + 1
    }
  }
}

object TreeNodeSuite {
  case class Node(value: Int, children: Seq[Node]) extends TreeNode[Node] {
    override def nodeCaption: String = s"Node($value)"

    def isLeaf: Boolean = children.isEmpty

    def wrongExists(f: Node => Boolean): Boolean = {
      transformDown {
        case node if f(node) && node.children.forall(_.isLeaf) => return true
        case node => node
      }
      false
    }
  }
}

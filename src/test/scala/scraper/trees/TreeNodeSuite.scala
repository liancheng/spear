package scraper.trees

import scala.collection.Iterator.iterate
import scala.language.implicitConversions

import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck.util.Pretty
import org.scalacheck.{Arbitrary, Gen, Test}
import org.scalatest.prop.Checkers

import scraper.LoggingFunSuite
import scraper.trees.TreeNodeSuite.Node
import scraper.types.TestUtils

class TreeNodeSuite extends LoggingFunSuite with TestUtils with Checkers {
  private def genNode: Gen[Node] = Gen.sized {
    case 1 =>
      Gen const Node(1, Nil)

    case size =>
      for {
        width <- Gen choose (1, size - 1)
        children <- Gen listOfN (width, Gen resize ((size - 1) / width, Gen lzy genNode))
      } yield Node(1, children)
  }

  implicit val arbNode = Arbitrary(genNode)

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
    val prop = forAll { tree: Node =>
      val even = tree collect { case n if n.children.size % 2 == 0 => n }
      val odd = tree collect { case n if n.children.size % 2 == 1 => n }
      val nodes = tree collect { case n => n }

      all(
        "all nodes are collected" |:
          (nodes.size == tree.size),

        "a node can't be both even and odd" |:
          (even intersect odd).isEmpty,

        "a node must be either even or odd" |:
          (even.size + odd.size == nodes.size),

        "even nodes are even" |:
          (even forall (_.children.size % 2 == 0)),

        "odd nodes are odd" |:
          (odd forall (_.children.size % 2 == 1))
      )
    }

    check(prop, Test.Parameters.defaultVerbose)
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
  }
}

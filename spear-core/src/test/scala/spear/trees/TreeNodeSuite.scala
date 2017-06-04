package spear.trees

import scala.collection.Iterator.iterate
import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.language.implicitConversions

import org.scalacheck._
import org.scalacheck.Prop.{all, BooleanOperators}
import org.scalacheck.util.Pretty
import org.scalatest.prop.Checkers

import spear.{LoggingFunSuite, TestUtils}
import spear.generators.genRandomPartitions
import spear.trees.TreeNodeSuite.Node

class TreeNodeSuite extends LoggingFunSuite with TestUtils with Checkers {
  def genNode: Gen[Node] = Gen.parameterized { param =>
    param.size match {
      case size if size < 2 => Node(1, Nil)
      case size =>
        for {
          width <- Gen choose (1, size - 1)
          childrenSizes <- genRandomPartitions(size - 1, width)
          children <- Gen sequence (childrenSizes map { Gen.resize(_, genNode) })
        } yield Node(1, children.asScala)
    }
  }

  implicit val arbNode = Arbitrary(genNode)

  implicit val shrinkNode: Shrink[Node] = Shrink {
    case node if node.isLeaf => Empty
    case node                => node.children.toStream :+ node.stripLeaves
  }

  implicit def prettyNode(tree: Node): Pretty = Pretty {
    _ => "\n" + tree.prettyTree
  }

  test("transformDown") {
    check {
      (_: Node) transformDown {
        case node @ Node(_, children) =>
          node.copy(value = children.map(_.value).sum)
      } forall {
        case Node(value, Nil)      => value == 0
        case Node(value, children) => value == children.size
      }
    }
  }

  test("transformUp") {
    check {
      (_: Node) transformUp {
        case n @ Node(_, children) =>
          n.copy(value = children.map(_.value).sum)
      } forall {
        case Node(value, _) => value == 0
      }
    }
  }

  test("collect") {
    check { tree: Node =>
      val even = tree collectDown { case n if n.children.size % 2 == 0 => n }
      val odd = tree collectDown { case n if n.children.size % 2 == 1 => n }
      val nodes = tree collectDown { case n => n }

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

  test("size") {
    check { tree: Node =>
      tree.size == (tree collectDown { case n => n.value }).sum
    }
  }

  test("depth") {
    check { tree: Node =>
      // Computes the depth by iterating over the tree and removing all the leaf nodes during each
      // iteration until only the root node is left.
      val iterations = iterate(tree) {
        _ transformDown { case n => n.stripLeaves }
      } takeWhile (_.size > 1)

      tree.depth == iterations.size + 1
    }
  }

  test("withChildren") {
    val children = (0 until 3) map { Node(_, Nil) }
    val node = Node(3, children)
    assert(node.withChildren(children) == node)
  }
}

object TreeNodeSuite {
  case class Node(value: Int, children: Seq[Node]) extends TreeNode[Node] {
    override def nodeCaption: String = s"Node($value)"

    def stripLeaves: Node =
      if (children forall (!_.isLeaf)) this else copy(children = children filterNot (_.isLeaf))
  }
}

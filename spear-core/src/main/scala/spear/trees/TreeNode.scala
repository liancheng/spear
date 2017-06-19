package spear.trees

import scala.collection.{mutable, Traversable}

/**
 * Base trait of simple tree structures that supports recursive transformations. Concrete $tree
 * classes are usually case classes with child $nodes defined as main constructor parameters:
 * {{{
 *   case class Tree1(value: String, left: Tree1, right: Tree1) extends TreeNode[Tree1] {
 *     def children: Seq[Tree1] = left :: right :: Nil
 *   }
 *
 *   case class Tree2(value: Int, children: Seq[Tree2]) extends TreeNode[Tree2]
 *
 *   case class Tree3(value: Double, maybeChild: Option[Tree3]) extends TreeNode[Tree3] {
 *     def children: Seq[Tree3] = maybeChild.toSeq
 *   }
 * }}}
 *
 * @define tree [[spear.trees.TreeNode tree]]
 * @define trees [[spear.trees.TreeNode trees]]
 * @define node [[spear.trees.TreeNode tree node]]
 * @define nodes [[spear.trees.TreeNode tree nodes]]
 * @define topDown Traverses this $tree in pre-order
 * @define bottomUp Traverses this $tree in post-order
 * @define transform transforms this $tree by applying a partial function that both accepts and
 *         returns a $tree to $nodes on which this function is defined.
 * @define transformChildren transforms child $nodes of this $tree by applying a partial function
 *         that both accepts and returns a $tree to $nodes on which this function is defined.
 */
trait TreeNode[Base <: TreeNode[Base]] extends Product { self: Base =>
  /** Returns all child $nodes of this $tree. */
  def children: Seq[Base]

  /** Whether this and `that` $tree point to the same reference or equal to each other. */
  def same(that: Base): Boolean = (this eq that) || this == that

  /** $topDown and $transform */
  def transformDown(rule: PartialFunction[Base, Base]): Base = {
    val newSelf = rule applyOrElse (this, identity[Base])
    newSelf transformChildren (rule, _ transformDown _)
  }

  /** $bottomUp and $transform */
  def transformUp(rule: PartialFunction[Base, Base]): Base = {
    val newSelf = this transformChildren (rule, _ transformUp _)
    rule applyOrElse (newSelf, identity[Base])
  }

  /** $topDown and $transformChildren */
  def transformChildrenDown(rule: PartialFunction[Base, Base]): Base =
    transformChildren(rule, _ transformDown _)

  /** $bottomUp and $transformChildren */
  def transformChildrenUp(rule: PartialFunction[Base, Base]): Base =
    transformChildren(rule, _ transformUp _)

  /** Returns the depth of this $tree. */
  def depth: Int = 1 + (children map { _.depth } foldLeft 0) { _ max _ }

  /** Returns the number of $nodes in this $tree. */
  def size: Int = 1 + children.map { _.size }.sum

  /** Whether this $node is a leaf node. */
  def isLeaf: Boolean = children.isEmpty

  /** Builds a copy of this $node with new child $nodes. */
  def withChildren(newChildren: Seq[Base]): Base = {
    assert(newChildren.length == children.length)

    val remainingNewChildren = newChildren.toBuffer
    var changed = false

    def popAndCompare(child: Any): Base = {
      val newChild = remainingNewChildren.head
      remainingNewChildren.remove(0)
      changed = changed || !(newChild same child.asInstanceOf[Base])
      newChild
    }

    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        popAndCompare(arg)

      case Some(arg) if children contains arg =>
        Some(popAndCompare(arg))

      case arg: Traversable[_] =>
        arg.map {
          case child: Any if children contains child => popAndCompare(child)
          case element                               => element
        }

      case arg: AnyRef =>
        arg

      case null =>
        null
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * $topDown and builds a sequence by applying a partial function to all $nodes in this $tree on
   * which the function is defined.
   */
  def collectDown[T](f: PartialFunction[Base, T]): Seq[T] = {
    val buffer = mutable.Buffer.empty[T]

    transformDown {
      case node if f.isDefinedAt(node) =>
        buffer += f(node)
        node
    }

    buffer
  }

  /**
   * $bottomUp and builds a sequence by applying a partial function to all $nodes in this $tree on
   * which the function is defined.
   */
  def collectUp[T](f: PartialFunction[Base, T]): Seq[T] = {
    val buffer = mutable.Buffer.empty[T]

    transformUp {
      case node if f.isDefinedAt(node) =>
        buffer += f(node)
        node
    }

    buffer
  }

  /**
   * $topDown and finds the first $node on which the given partial function is defined and applies
   * the partial function to it.
   */
  def collectFirstDown[T](f: PartialFunction[Base, T]): Option[T] = {
    transformDown {
      case node if f.isDefinedAt(node) =>
        return Some(f(node))
    }

    None
  }

  /** Returns `true` if `f` is `true` for all nodes in this tree. */
  def forall(f: Base => Boolean): Boolean = {
    transformDown {
      case node if f(node) => node
      case _               => return false
    }
    true
  }

  /** Returns `true` if `f` is `true` for at least one node in this tree. */
  def exists(f: Base => Boolean): Boolean = {
    transformDown {
      case node if f(node) => return true
      case node            => node
    }
    false
  }

  override def toString: String = "\n" + prettyTree

  /**
   * Returns a pretty-printed tree string for this $tree.
   */
  def prettyTree: String = buildPrettyTree(0, Nil, StringBuilder.newBuilder).toString.trim

  /**
   * Returns a single-line string representation of this $tree when it is shown as a node in a
   * pretty-printed tree string.
   *
   * @see [[prettyTree]]
   */
  def caption: String = nodeName

  /** Name of this $node. */
  def nodeName: String = getClass.getSimpleName stripSuffix "$"

  protected def makeCopy(args: Seq[AnyRef]): Base = {
    val constructors = this.getClass.getConstructors filter { _.getParameterTypes.nonEmpty }
    assert(constructors.nonEmpty, s"No valid constructor for ${getClass.getSimpleName}")

    try {
      val defaultConstructor = constructors.maxBy(_.getParameterTypes.length)
      defaultConstructor.newInstance(args: _*).asInstanceOf[Base]
    } catch {
      case cause: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Failed to instantiate ${getClass.getName}", cause)
    }
  }

  private type Rule = PartialFunction[Base, Base]

  private def transformChildren(rule: Rule, next: (Base, Rule) => Base): Base = {
    val newChildren = children map { next(_, rule) }
    val anyChildChanged = (newChildren, children).zipped map { _ same _ } contains false
    if (anyChildChanged) withChildren(newChildren) else this
  }

  /**
   * Pretty prints this [[TreeNode]] and all of its offsprings in the form of a tree.
   *
   * @param depth Depth of the current node.  Depth of the root node is 0.
   * @param youngest The `i`-th element in `youngest` indicates whether the direct ancestor
   *        of this [[TreeNode]] at depth `i + 1` is the last child of its own parent at depth `i`.
   *        For root node, `youngest` is empty (`Nil`).
   * @param builder The string builder used to build the tree string.
   */
  def buildPrettyTree(
    depth: Int, youngest: Seq[Boolean], builder: StringBuilder
  ): StringBuilder = {
    val captionLines = caption split "\n"

    // Writes the first line of the caption.
    if (depth > 0) {
      youngest.init foreach { isLast => builder ++= (if (isLast) "  " else s"│ ") }
      builder ++= (if (youngest.last) s"╰╴" else s"├╴")
    }

    builder ++= captionLines.head
    builder += '\n'

    // Writes the rest lines of the caption, if any.
    captionLines.tail foreach { line =>
      if (depth > 0) {
        youngest foreach { isLast => builder ++= (if (isLast) "  " else s"│ ") }
      }

      builder ++= line
      builder += '\n'
    }

    // Writes child nodes, if any.
    if (children.nonEmpty) {
      children.init foreach { _ buildPrettyTree (depth + 1, youngest :+ false, builder) }
      children.last buildPrettyTree (depth + 1, youngest :+ true, builder)
    }

    builder
  }
}

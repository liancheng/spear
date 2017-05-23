package spear.trees

import scala.collection.{mutable, Traversable}

import spear.annotations.Explain
import spear.reflection.constructorParams

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
 * @define topDown Traverses this $tree top-down
 * @define bottomUp Traverses this $tree bottom-up
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

    val newArgs = productIterator.map {
      case arg: TreeNode[_] if children contains arg =>
        val newChild = remainingNewChildren.head
        remainingNewChildren.remove(0)
        changed = changed || !(newChild same arg.asInstanceOf[Base])
        newChild

      case Some(arg) if children contains arg =>
        val newChild = remainingNewChildren.head
        remainingNewChildren.remove(0)
        changed = changed || !(newChild same arg.asInstanceOf[Base])
        Some(newChild)

      case arg: Traversable[_] =>
        arg.map {
          case child: Any if children contains child =>
            val newChild = remainingNewChildren.head
            remainingNewChildren.remove(0)
            changed = changed || !(newChild same child.asInstanceOf[Base])
            newChild

          case element =>
            element
        }

      case arg: AnyRef =>
        arg

      case null =>
        null
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Builds a sequence by applying a partial function to all $nodes in this $tree on which the
   * function is defined.
   */
  def collect[T](f: PartialFunction[Base, T]): Seq[T] = {
    val buffer = mutable.Buffer.empty[T]

    transformDown {
      case node if f.isDefinedAt(node) =>
        buffer += f(node)
        node
    }

    buffer
  }

  /**
   * Finds the first $node on which the given partial function is defined and applies the partial
   * function to it.
   */
  def collectFirst[T](f: PartialFunction[Base, T]): Option[T] = {
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
  def nodeCaption: String = {
    val pairs = explainParams(_.toString).map { _.productIterator mkString "=" }
    Seq(nodeName, pairs mkString ", ") filter { _.nonEmpty } mkString " "
  }

  /** Name of this $node. */
  def nodeName: String = getClass.getSimpleName stripSuffix "$"

  protected def makeCopy(args: Seq[AnyRef]): Base = {
    val constructors = this.getClass.getConstructors filter { _.getParameterTypes.nonEmpty }
    assert(constructors.nonEmpty, s"No valid constructor for ${getClass.getSimpleName}")
    val defaultConstructor = constructors.maxBy(_.getParameterTypes.length)
    try defaultConstructor.newInstance(args: _*).asInstanceOf[Base] catch {
      case cause: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Failed to instantiate ${getClass.getName}", cause)
    }
  }

  protected def nestedTrees: Seq[TreeNode[_]] = {
    val nestedTreeMarks = constructorParamExplainAnnotations map { _ exists { _.nestedTree() } }
    productIterator.toSeq zip nestedTreeMarks collect { case (tree: TreeNode[_], true) => tree }
  }

  /**
   * String pairs representing constructor parameters of this $tree. Parameters annotated with
   * `Explain(hidden = true)` are not included here.
   */
  protected def explainParams(show: Any => String): Seq[(String, String)] = {
    val argNames: List[String] = constructorParams(getClass) map { _.name.toString }
    val annotations = constructorParamExplainAnnotations

    (argNames, productIterator.toSeq, annotations).zipped.map {
      case (_, _, maybeAnnotated) if maybeAnnotated exists { _.hidden() } =>
        None

      case (name, value, _) =>
        explainParamValue(value, show) map { name -> _ }
    }.flatten
  }

  private type Rule = PartialFunction[Base, Base]

  private lazy val constructorParamExplainAnnotations: Seq[Option[Explain]] =
    getClass.getDeclaredConstructors.head.getParameterAnnotations.map {
      _ collectFirst { case a: Explain => a }
    }

  private def transformChildren(rule: Rule, next: (Base, Rule) => Base): Base = {
    val newChildren = children map { next(_, rule) }
    val anyChildChanged = (newChildren, children).zipped map { _ same _ } contains false
    if (anyChildChanged) withChildren(newChildren) else this
  }

  /**
   * Pretty prints this [[TreeNode]] and all of its offsprings in the form of a tree.
   *
   * @param depth Depth of the current node.  Depth of the root node is 0.
   * @param lastChildren The `i`-th element in `lastChildren` indicates whether the direct ancestor
   *        of this [[TreeNode]] at depth `i + 1` is the last child of its own parent at depth `i`.
   *        For root node, `lastChildren` is empty (`Nil`).
   * @param builder The string builder used to build the tree string.
   */
  private def buildPrettyTree(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder
  ): StringBuilder = {
    val pipe = "\u2502"
    val tee = "\u251c"
    val corner = "\u2570"
    val bar = "\u2574"

    if (depth > 0) {
      lastChildren.init foreach (isLast => builder ++= (if (isLast) "  " else s"$pipe "))
      builder ++= (if (lastChildren.last) s"$corner$bar" else s"$tee$bar")
    }

    builder ++= nodeCaption
    builder ++= "\n"

    buildNestedTree(depth, lastChildren, builder)

    if (children.nonEmpty) {
      children.init foreach { _ buildPrettyTree (depth + 1, lastChildren :+ false, builder) }
      children.last buildPrettyTree (depth + 1, lastChildren :+ true, builder)
    }

    builder
  }

  private def buildNestedTree(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder
  ): Unit = if (nestedTrees.nonEmpty) {
    nestedTrees.init.foreach {
      _.buildPrettyTree(depth + 2, lastChildren :+ children.isEmpty :+ false, builder)
    }

    nestedTrees.last.buildPrettyTree(depth + 2, lastChildren :+ children.isEmpty :+ true, builder)
  }

  private def explainParamValue(value: Any, show: Any => String): Option[String] = value match {
    case arg if children contains arg =>
      // Hides child nodes since they will be printed as sub-tree nodes
      None

    case arg: Seq[_] if arg forall children.contains =>
      // If a `Seq` contains only child nodes, hides it entirely.
      None

    case arg: Seq[_] =>
      Some {
        arg flatMap {
          case e if children contains e => None
          case e                        => Some(show(e))
        } mkString ("[", ", ", "]")
      }

    case arg: Some[_] =>
      arg flatMap {
        case e: Any if children contains e => None
        case e: Any                        => Some("Some(" + show(e) + ")")
      }

    case arg =>
      Some(show(arg))
  }
}

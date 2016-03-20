package scraper.trees

import scala.collection.{mutable, Traversable}
import scala.languageFeature.reflectiveCalls

trait TreeNode[Base <: TreeNode[Base]] extends Product { self: Base =>
  private type Rule = PartialFunction[Base, Base]

  def children: Seq[Base]

  protected def sameChildren(newChildren: Seq[Base]): Boolean =
    (newChildren, children).zipped forall (_ same _)

  /**
   * Returns whether this [[nodeCaption]] and `that` point to the same reference or equal
   * to each other.
   */
  def same(that: Base): Boolean = (this eq that) || this == that

  def transformDown(rule: PartialFunction[Base, Base]): Base = {
    val selfTransformed = rule applyOrElse (this, identity[Base])
    selfTransformed transformChildren (rule, _ transformDown _)
  }

  def transformUp(rule: PartialFunction[Base, Base]): Base = {
    val childrenTransformed = this transformChildren (rule, _ transformUp _)
    rule applyOrElse (childrenTransformed, identity[Base])
  }

  def transformChildrenDown(rule: PartialFunction[Base, Base]): Base =
    this transformChildren (rule, _ transformDown _)

  def transformChildrenUp(rule: PartialFunction[Base, Base]): Base =
    this transformChildren (rule, _ transformUp _)

  private def transformChildren(rule: Rule, next: (Base, Rule) => Base): Base = {
    // Returns the transformed tree and a boolean flag indicating whether the transformed tree is
    // equivalent to the original one
    def applyRule(tree: Base): (Base, Boolean) = {
      val transformed = next(tree, rule)
      if (tree same transformed) tree -> false else transformed -> true
    }

    val (newArgs, argsChanged) = productIterator.map {
      case t: TreeNode[_] if children contains t =>
        applyRule(t.asInstanceOf[Base])

      case Some(t: TreeNode[_]) if children contains t =>
        val (ruleApplied, transformed) = applyRule(t.asInstanceOf[Base])
        Some(ruleApplied) -> transformed

      case arg: Traversable[_] =>
        val (newElements, elementsChanged) = arg.map {
          case node: TreeNode[_] if children contains node => applyRule(node.asInstanceOf[Base])
          case element                                     => element -> false
        }.unzip
        newElements -> (elementsChanged exists (_ == true))

      case arg: AnyRef =>
        arg -> false

      case null =>
        (null, false)
    }.toSeq.unzip

    if (argsChanged contains true) makeCopy(newArgs) else this
  }

  protected def makeCopy(args: Seq[AnyRef]): Base = {
    val constructors = this.getClass.getConstructors.filter(_.getParameterTypes.nonEmpty)
    assert(constructors.nonEmpty, s"No valid constructor for ${getClass.getSimpleName}")
    val defaultConstructor = constructors.maxBy(_.getParameterTypes.length)
    try {
      defaultConstructor.newInstance(args: _*).asInstanceOf[Base]
    } catch {
      case cause: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Failed to instantiate ${getClass.getName}", cause)
    }
  }

  def collect[T](f: PartialFunction[Base, T]): Seq[T] = {
    val buffer = mutable.Buffer.empty[T]

    transformDown {
      case node if f.isDefinedAt(node) =>
        buffer += f(node)
        node
    }

    buffer
  }

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

  def prettyTree: String =
    buildPrettyTree(0, Nil, StringBuilder.newBuilder).toString.trim

  def nodeCaption: String = toString

  override def toString: String = "\n" + prettyTree

  def nodeName: String = getClass.getSimpleName stripSuffix "$"

  /**
   * Pretty prints this [[TreeNode]] and all of its offsprings in the form of a tree.
   *
   * @param depth Depth of the current node.  Depth of the root node is 0.
   * @param lastChildren The `i`-th element in `lastChildren` indicates whether the direct ancestor
   *        of this [[TreeNode]] at depth `i + 1` is the last child of its own parent at depth `i`.
   *        For root node, `lastChildren` is empty (`Nil`).
   * @param builder The string builder used to build the tree string.
   */
  protected[scraper] def buildPrettyTree(
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

    buildVirtualTreeNodes(depth, lastChildren, builder)

    if (children.nonEmpty) {
      children.init foreach (_ buildPrettyTree (depth + 1, lastChildren :+ false, builder))
      children.last buildPrettyTree (depth + 1, lastChildren :+ true, builder)
    }

    builder
  }

  /**
   * Adds "virtual" nodes that are not members of [[children]] to the pretty printed tree string.
   * Used by [[scraper.plans.QueryPlan QueryPlan]] to bake expression tree nodes.
   *
   * @see [[buildPrettyTree]] for more details.
   */
  protected def buildVirtualTreeNodes(
    depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder
  ): Unit = ()

  def depth: Int = 1 + (children map (_.depth) foldLeft 0) { _ max _ }

  def size: Int = 1 + children.map(_.size).sum

  def isLeaf: Boolean = children.isEmpty
}

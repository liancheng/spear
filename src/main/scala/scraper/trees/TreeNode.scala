package scraper.trees

import scala.collection.{Traversable, mutable}
import scala.languageFeature.reflectiveCalls

trait TreeNode[Base <: TreeNode[Base]] extends Product { self: Base =>
  private type Rule = PartialFunction[Base, Base]

  def children: Seq[Base]

  /**
   * Returns whether this [[caption]] and `that` point to the same reference or equal to each
   * other.
   */
  def sameOrEqual(that: Base): Boolean = (this eq that) || this == that

  def transformDown(rule: PartialFunction[Base, Base]): Base = {
    val transformedSelf = rule applyOrElse (this, identity[Base])
    transformedSelf transformChildren (rule, _ transformDown _)
  }

  def transformUp(rule: PartialFunction[Base, Base]): Base = {
    val childrenTransformed = this transformChildren (rule, _ transformUp _)
    rule applyOrElse (childrenTransformed, identity[Base])
  }

  private def transformChildren(rule: Rule, next: (Base, Rule) => Base): this.type = {
    // Returns the transformed tree and a boolean flag indicating whether the transformed tree is
    // equivalent to the original one
    def applyRule(tree: Base): (Base, Boolean) = {
      val transformed = next(tree, rule)
      if (tree sameOrEqual transformed) tree -> false else transformed -> true
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
        newElements -> (elementsChanged exists identity)

      case arg: AnyRef =>
        arg -> false
    }.toSeq.unzip

    if (argsChanged exists identity) makeCopy(newArgs) else this
  }

  private[scraper] def makeCopy(args: Seq[AnyRef]): this.type = {
    val constructors = this.getClass.getConstructors.filter(_.getParameterTypes.nonEmpty)
    assert(constructors.nonEmpty, s"No valid constructor for ${getClass.getSimpleName}")
    val defaultConstructor = constructors.maxBy(_.getParameterTypes.length)
    try {
      defaultConstructor.newInstance(args: _*).asInstanceOf[this.type]
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

    buffer.toSeq
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

  def prettyTree: String = prettyTree(0, Nil) mkString "\n"

  def caption: String = toString

  private def prettyTree(depth: Int, isLastChild: Seq[Boolean]): Seq[String] = {
    val pipe = "\u2502"
    val tee = "\u251c"
    val corner = "\u2514"
    val bar = "\u2574"

    val prefix = if (depth == 0) {
      Seq.empty
    } else {
      isLastChild.init.map { isLast =>
        if (isLast) " " * 2 else s"$pipe "
      } :+ (if (isLastChild.last) s"$corner$bar" else s"$tee$bar")
    }

    val head = Seq(prefix.mkString + caption)

    if (children.isEmpty) {
      head
    } else {
      val body = children.init.flatMap(_.prettyTree(depth + 1, isLastChild :+ false))
      val last = children.last.prettyTree(depth + 1, isLastChild :+ true)
      head ++ body ++ last
    }
  }

  def depth: Int = (children map (_.depth) foldLeft 1) { _ max _ }

  def size: Int = (children map (_.size) foldLeft 1) { _ + _ }
}

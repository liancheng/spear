package scraper.expressions.dsl

import scraper.expressions.Expression
import scraper.expressions.`object`.Invoke
import scraper.types.DataType

trait ObjectDSL { this: Expression =>
  def invoke: InvokeBuilder = new InvokeBuilder(this)
}

class InvokeBuilder(target: Expression) {
  private[this] var name: String = _

  private[this] var returnType: DataType = _

  def method(name: String): this.type = {
    this.name = name
    this
  }

  def of(returnType: DataType): this.type = {
    this.returnType = returnType
    this
  }

  def withArgs(args: Expression*): Invoke = Invoke(target, name, returnType, args)
}

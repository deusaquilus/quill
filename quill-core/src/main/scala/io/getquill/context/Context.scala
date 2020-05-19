package io.getquill.context

import scala.language.higherKinds
import scala.language.experimental.macros
import io.getquill.dsl.CoreDsl
import io.getquill.util.Messages.fail
import java.io.Closeable

import scala.util.Try
import io.getquill.{ Query, Action, NamingStrategy, BatchAction, ReturnAction, ActionReturning }

trait SimplePrepareContext extends CoreDsl {
  type Result[T]
  type Session

  type PrepareQueryResult = Session => Result[PrepareRow]
  type PrepareActionResult = Session => Result[PrepareRow]
  type PrepareBatchActionResult = Session => Result[List[PrepareRow]]
}

trait Context[Idiom <: io.getquill.idiom.Idiom, Naming <: NamingStrategy]
  extends Closeable
  with CoreDsl {

  type Result[T]
  type RunQuerySingleResult[T]
  type RunQueryResult[T]
  type RunActionResult
  type RunActionReturningResult[T]
  type RunBatchActionResult
  type RunBatchActionReturningResult[T]
  type Session
  type PrepareQueryResult //Usually: Session => Result[PrepareRow]
  type PrepareActionResult //Usually: Session => Result[PrepareRow]
  type PrepareBatchActionResult //Usually: Session => Result[List[PrepareRow]]

  type Prepare = PrepareRow => (List[Any], PrepareRow)
  type Extractor[T] = ResultRow => T

  case class BatchGroup(string: String, prepare: List[Prepare])
  case class BatchGroupReturning(string: String, returningBehavior: ReturnAction, prepare: List[Prepare])

  def probe(statement: String): Try[_]

  def idiom: Idiom
  def naming: Naming

  def run[T](quoted: Quoted[T]): Result[RunQuerySingleResult[T]] = macro QueryMacro.runQuerySingle[T]
  def run[T](quoted: Quoted[Query[T]]): Result[RunQueryResult[T]] = macro QueryMacro.runQuery[T]
  def prepare[T](quoted: Quoted[Query[T]]): PrepareQueryResult = macro QueryMacro.prepareQuery[T]

  def run(quoted: Quoted[Action[_]]): Result[RunActionResult] = macro ActionMacro.runAction
  def run[T](quoted: Quoted[ActionReturning[_, T]]): Result[RunActionReturningResult[T]] = macro ActionMacro.runActionReturning[T]
  def run(quoted: Quoted[BatchAction[Action[_]]]): Result[RunBatchActionResult] = macro ActionMacro.runBatchAction
  def run[T](quoted: Quoted[BatchAction[ActionReturning[_, T]]]): Result[RunBatchActionReturningResult[T]] = macro ActionMacro.runBatchActionReturning[T]
  def prepare(quoted: Quoted[Action[_]]): PrepareActionResult = macro ActionMacro.prepareAction
  def prepare(quoted: Quoted[BatchAction[Action[_]]]): PrepareBatchActionResult = macro ActionMacro.prepareBatchAction

  protected val identityPrepare: Prepare = (Nil, _)
  protected val identityExtractor = identity[ResultRow] _

  protected def handleSingleResult[T](list: List[T]) =
    list match {
      case value :: Nil => value
      case other        => fail(s"Expected a single result but got $other")
    }
}

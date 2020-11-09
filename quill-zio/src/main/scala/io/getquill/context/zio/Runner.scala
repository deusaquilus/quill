package io.getquill.context.zio

import java.sql.SQLException

import io.getquill.context.ContextEffect
import zio.{ Task, ZIO, RIO }
import zio.internal.Executor

object Runner {
  type RIOConn[T] = RIO[java.sql.Connection, T]

  def default = new Runner {}
  def using(executor: Executor) = new Runner {
    override def schedule[T](t: Task[T]): Task[T] = t.lock(executor)
    override def boundary[T](t: Task[T]): Task[T] = Task.yieldNow *> t.lock(executor)
  }
}

trait Runner extends ContextEffect[Runner.RIOConn] {
  override def wrap[T](t: => T): Runner.RIOConn[T] = Task(t)
  override def push[A, B](result: Runner.RIOConn[A])(f: A => B): Runner.RIOConn[B] = result.map(f)
  override def seq[A](list: List[Runner.RIOConn[A]]): Runner.RIOConn[List[A]] = ZIO.collectAll[java.sql.Connection, Throwable, A, List](list)
  def schedule[T](t: Task[T]): Runner.RIOConn[T] = t
  def boundary[T](t: Task[T]): Runner.RIOConn[T] = Task.yieldNow *> t

  def catchAll[T, R](task: ZIO[R, Throwable, T]): ZIO[R, Nothing, Any] = task.catchAll {
    case _: SQLException              => Task.unit // TODO Log something. Can't have anything in the error channel... still needed
    case _: IndexOutOfBoundsException => Task.unit
    case e                            => Task.die(e): ZIO[Any, Nothing, T]
  }

  /**
   * Use this method whenever a ResultSet is being wrapped. This has a distinct
   * method because the client may prefer to fail silently on a ResultSet close
   * as opposed to failing the surrounding task.
   */
  def wrapClose(t: => Any): ZIO[java.sql.Connection, Nothing, Unit] = catchAll(Task(t)).unit
}

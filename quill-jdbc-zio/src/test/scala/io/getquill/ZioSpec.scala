package io.getquill

import java.sql.Connection

import io.getquill.context.zio.ZioJdbcContext
import zio.stream.ZStream

trait ZioSpec extends Spec {

  //implicit val scheduler = Scheduler.global

  val context: ZioJdbcContext[_, _] with TestEntities

  def accumulate[T](o: ZStream[Connection, Throwable, T]) =
    o.toIterator.map(_.toList)

  //  def collect[T](o: ZStream[Nothing, Throwable, T]) =
  //    accumulate(o)
}

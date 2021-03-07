package io.getquill

import io.getquill.context.zio.ZioJdbcContext.{ BlockingConnection, _ }
import zio.{ Runtime, ZIO }
import zio.stream.{ Sink, ZStream }

trait ZioSpec extends Spec {
  def prefix: Prefix;

  def accumulate[T](stream: ZStream[BlockingConnection, Throwable, T]): ZIO[BlockingConnection, Throwable, List[T]] =
    stream.run(Sink.collectAll).map(_.toList)

  def collect[T](stream: ZStream[BlockingConnection, Throwable, T]): List[T] =
    Runtime.default.unsafeRun(stream.run(Sink.collectAll).map(_.toList).providePrefix(prefix))

  def collect[T](qzio: ZIO[BlockingConnection, Throwable, T]): T =
    Runtime.default.unsafeRun(qzio.providePrefix(prefix))

  implicit class ZStreamTestExt[T](stream: ZStream[BlockingConnection, Throwable, T]) {
    def runSyncUnsafe() = collect[T](stream)
  }

  implicit class ZioTestExt[T](qzio: ZIO[BlockingConnection, Throwable, T]) {
    def runSyncUnsafe() = collect[T](qzio)
  }
}

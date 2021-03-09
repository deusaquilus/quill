package io.getquill.context.zio

import io.getquill.NamingStrategy
import io.getquill.context.{ Context, StreamingContext }
import zio.blocking.Blocking
import zio.stream.ZStream
import zio.{ Has, RIO, Task, ZIO }

import java.sql.SQLException

trait ZioContext[Idiom <: io.getquill.idiom.Idiom, Naming <: NamingStrategy] extends Context[Idiom, Naming]
  with StreamingContext[Idiom, Naming] {

  // It's nice that we don't actually have to import any JDBC libraries to have a Connection type here
  override type StreamResult[T] = ZStream[Has[Session] with Blocking, Throwable, T]
  override type Result[T] = RIO[Has[Session] with Blocking, T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Has[Session] with Blocking, List[T]]
  def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Has[Session] with Blocking, T]
}

object ZioCatchAll extends ZioCatchAll
trait ZioCatchAll {
  private[getquill] def catchAll[T, R](task: ZIO[R, Throwable, T]): ZIO[R, Nothing, Any] = task.catchAll {
    case _: SQLException              => Task.unit // TODO Log something. Can't have anything in the error channel... still needed
    case _: IndexOutOfBoundsException => Task.unit
    case e                            => Task.die(e): ZIO[Any, Nothing, T]
  }
}

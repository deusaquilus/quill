package io.getquill.context.zio

import io.getquill.NamingStrategy
import io.getquill.context.{ Context, StreamingContext }
import zio.RIO
import zio.stream.ZStream
import java.sql.Connection

trait ZioContext[Idiom <: io.getquill.idiom.Idiom, Naming <: NamingStrategy] extends Context[Idiom, Naming]
  with StreamingContext[Idiom, Naming] {

  // It's nice that we don't actually have to import any JDBC libraries to have a Connection type here
  override type StreamResult[T] = ZStream[Connection, Throwable, T]
  override type Result[T] = RIO[Connection, T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Connection, List[T]]
  def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Connection, T]
}

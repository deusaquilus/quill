package io.getquill.context.zio

import com.typesafe.config.Config

import java.sql.{ Array => _, _ }
import io.getquill.{ JdbcContextConfig, NamingStrategy, ReturnAction }
import io.getquill.context.StreamingContext
import io.getquill.context.jdbc.JdbcContextSimplified
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.{ ContextLogger, LoadConfig }

import javax.sql.DataSource
import zio.Exit.{ Failure, Success }
import zio.{ Chunk, ChunkBuilder, Has, IO, RIO, Task, UIO, ZIO, ZLayer, ZManaged }
import zio.stream.{ Stream, ZStream }
import ZioContext._
import izumi.reflect.Tag

import scala.util.Try

/**
 * Quill context that wraps all JDBC calls in `monix.eval.Task`.
 *
 */
abstract class ZioJdbcContext[Dialect <: SqlIdiom, Naming <: NamingStrategy] extends ZioContext[Dialect, Naming]
  with JdbcContextSimplified[Dialect, Naming]
  with StreamingContext[Dialect, Naming]
  with ZioTranslateContext {

  override private[getquill] val logger = ContextLogger(classOf[ZioJdbcContext[_, _]])

  // TODO Need to refactor out. Difficult because JdbcContextSimplified class defines this as Connection => ResultRow[PreparedStatement]
  override def prepareSingle(sql: String, prepare: Prepare): Connection => RIO[BlockingConnection, PreparedStatement] = super.prepareSingle(sql, prepare)

  override type PrepareRow = PreparedStatement
  override type ResultRow = ResultSet
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]
  override type PrepareQueryResult = RIO[BlockingConnection, PrepareRow]
  override type PrepareActionResult = RIO[BlockingConnection, PrepareRow]
  override type PrepareBatchActionResult = RIO[BlockingConnection, List[PrepareRow]]

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  override def executeAction[T](sql: String, prepare: Prepare = identityPrepare): RIO[BlockingConnection, Long] =
    super.executeAction(sql, prepare)
  override def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[BlockingConnection, List[T]] =
    super.executeQuery(sql, prepare, extractor)
  override def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[BlockingConnection, T] =
    super.executeQuerySingle(sql, prepare, extractor)
  override def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningBehavior: ReturnAction): RIO[BlockingConnection, O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)
  override def executeBatchAction(groups: List[BatchGroup]): RIO[BlockingConnection, List[Long]] =
    super.executeBatchAction(groups)
  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): RIO[BlockingConnection, List[T]] =
    super.executeBatchActionReturning(groups, extractor)
  override def prepareQuery[T](sql: String, prepare: Prepare, extractor: Extractor[T] = identityExtractor): RIO[BlockingConnection, PreparedStatement] =
    super.prepareQuery(sql, prepare, extractor)
  override def prepareAction(sql: String, prepare: Prepare): RIO[BlockingConnection, PreparedStatement] =
    super.prepareAction(sql, prepare)
  override def prepareBatchAction(groups: List[BatchGroup]): RIO[BlockingConnection, List[PreparedStatement]] =
    super.prepareBatchAction(groups)

  override protected val effect: Runner = Runner.default
  import effect._

  /** ZIO Contexts do not managed DB connections so this is a no-op */
  override def close(): Unit = ()

  protected def withConnection[T](f: Connection => Result[T]): Result[T] = throw new IllegalArgumentException("Not Used")
  override protected def withConnectionWrapped[T](f: Connection => T): RIO[BlockingConnection, T] =
    RIO.fromFunction((conn: BlockingConnection) => f(conn.get))

  trait SameThreadExecutionContext extends scala.concurrent.ExecutionContext {
    def submit(runnable: Runnable): Unit =
      runnable.run()
  }

  def transaction[A](f: RIO[BlockingConnection, A]): RIO[BlockingConnection, A] = {
    def die = Task.die(new IllegalStateException("The task was cancelled in the middle of a transaction."))
    ZIO.environment[BlockingConnection].flatMap(conn =>
      f.onInterrupt(die).onExit {
        case Success(_) =>
          UIO(conn.get.commit())
        case Failure(cause) =>
          // TODO Are we really catching the result of the conn.rollback() Task's exception?
          catchAll(Task(conn.get.rollback()) *> Task.halt(cause))
      })
  }

  def probingDataSource: Option[DataSource] = None

  // Override with sync implementation so will actually be able to do it.
  override def probe(sql: String): Try[_] =
    probingDataSource match {
      case Some(dataSource) =>
        Try {
          val c = dataSource.getConnection
          try {
            c.createStatement().execute(sql)
          } finally {
            c.close()
          }
        }
      case None => Try[Unit](())
    }

  /**
   * In order to allow a ResultSet to be consumed by an Observable, a ResultSet iterator must be created.
   * Since Quill provides a extractor for an individual ResultSet row, a single row can easily be cached
   * in memory. This allows for a straightforward implementation of a hasNext method.
   */
  class ResultSetIterator[T](rs: ResultSet, extractor: Extractor[T]) extends BufferedIterator[T] {

    private[this] var state = 0 // 0: no data, 1: cached, 2: finished
    private[this] var cached: T = null.asInstanceOf[T]

    protected[this] final def finished(): T = {
      state = 2
      null.asInstanceOf[T]
    }

    /** Return a new value or call finished() */
    protected def fetchNext(): T =
      if (rs.next()) extractor(rs)
      else finished()

    def head: T = {
      prefetchIfNeeded()
      if (state == 1) cached
      else throw new NoSuchElementException("head on empty iterator")
    }

    private def prefetchIfNeeded(): Unit = {
      if (state == 0) {
        cached = fetchNext()
        if (state == 0) state = 1
      }
    }

    def hasNext: Boolean = {
      prefetchIfNeeded()
      state == 1
    }

    def next(): T = {
      prefetchIfNeeded()
      if (state == 1) {
        state = 0
        cached
      } else throw new NoSuchElementException("next on empty iterator");
    }
  }

  /**
   * Override to enable specific vendor options needed for streaming
   */
  protected def prepareStatementForStreaming(sql: String, conn: Connection, fetchSize: Option[Int]) = {
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    fetchSize.foreach { size =>
      stmt.setFetchSize(size)
    }
    stmt
  }

  def streamQuery[T](fetchSize: Option[Int], sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): ZStream[BlockingConnection, Throwable, T] = {
    def prepareStatement(conn: Connection) = {
      val stmt = prepareStatementForStreaming(sql, conn, fetchSize)
      val (params, ps) = prepare(stmt)
      logger.logQuery(sql, params)
      ps
    }

    val managedEnv: ZStream[Connection, Throwable, (Connection, PrepareRow, ResultSet)] =
      ZStream.environment[Connection].flatMap { conn =>
        ZStream.managed {
          for {
            conn <- ZManaged.make(Task(conn))(c => Task.unit)
            ps <- ZManaged.make(Task(prepareStatement(conn)))(ps => wrapClose(ps.close()))
            rs <- ZManaged.make(Task(ps.executeQuery()))(rs => wrapClose(rs))
          } yield (conn, ps, rs)
        }
      }

    val outStream: ZStream[Connection, Throwable, T] =
      managedEnv.flatMap {
        case (conn, ps, rs) =>
          val iter = new ResultSetIterator(rs, extractor)
          fetchSize match {
            // TODO Assuming chunk size is fetch size. Not sure if this is optimal.
            //      Maybe introduce some switches to control this?
            case Some(size) =>
              chunkedFetch(iter, size)
            case None =>
              Stream.fromIterator(new ResultSetIterator(rs, extractor))
          }
      }

    // TODO This is a trick to convert Connection to BlockingConnection. Is there a better way to do that e.g. something like contraMapEnv
    val wrapper = (bc: BlockingConnection) => outStream.provide(bc.get)
    ZStream.access[BlockingConnection](wrapper).flatten
  }

  def guardedChunkFill[A](n: Int)(hasNext: => Boolean, elem: => A): Chunk[A] =
    if (n <= 0) Chunk.empty
    else {
      val builder = ChunkBuilder.make[A]()
      builder.sizeHint(n)

      var i = 0
      while (i < n && hasNext) {
        builder += elem
        i += 1
      }
      builder.result()
    }

  def chunkedFetch[T](iter: ResultSetIterator[T], fetchSize: Int) = {
    object StreamEnd extends Throwable
    ZStream.fromEffect(Task(iter) <*> ZIO.runtime[Any]).flatMap {
      case (it, rt) =>
        ZStream.repeatEffectChunkOption {
          Task {
            val hasNext: Boolean =
              try it.hasNext
              catch {
                case e: Throwable if !rt.platform.fatal(e) =>
                  throw e
              }
            if (hasNext) {
              try {
                // The most efficent way to load an array is to allocate a slice that has the number of elements
                // that will be returned by every database fetch i.e. the fetch size. Since the later iteration
                // may return fewer elements then that, we need a special guard for that particular scenario.
                // However, since we do not know which slice is that last, the guard (i.e. hasNext())
                // needs to be used for all of them.
                guardedChunkFill(fetchSize)(it.hasNext, it.next())
              } catch {
                case e: Throwable if !rt.platform.fatal(e) =>
                  throw e
              }
            } else throw StreamEnd
          }.mapError {
            case StreamEnd => None
            case e         => Some(e)
          }
        }
    }
  }

  override private[getquill] def prepareParams(statement: String, prepare: Prepare): RIO[BlockingConnection, Seq[String]] = {
    withConnectionWrapped { conn =>
      prepare(conn.prepareStatement(statement))._1.reverse.map(prepareParam)
    }
  }

  def constructPrepareQuery(f: Connection => Result[PreparedStatement]): RIO[BlockingConnection, PreparedStatement] =
    ZIO.environment[BlockingConnection].flatMap(c => f(c.get))

  def constructPrepareAction(f: Connection => Result[PreparedStatement]): RIO[BlockingConnection, PreparedStatement] =
    ZIO.environment[BlockingConnection].flatMap(c => f(c.get))

  def constructPrepareBatchAction(f: Connection => Result[List[PreparedStatement]]): RIO[BlockingConnection, List[PreparedStatement]] =
    ZIO.environment[BlockingConnection].flatMap(c => f(c.get))
}

object ZioJdbcContext {
  import zio.blocking._

  val defaultRunner = io.getquill.context.zio.Runner.default

  //ZLayer.fromEffectMany(effectBlocking(LoadConfig(str)))
  type BlockingDataSource = Has[DataSource] with Blocking
  type BlockingJdbcContextConfig = Has[JdbcContextConfig] with Blocking
  type BlockingConfig = Has[Config] with Blocking
  type BlockingPrefix = Has[Prefix] with Blocking

  def createFromDataSource[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingDataSource, Throwable, T] =
    ZIO.environment[BlockingDataSource].flatMap { bd =>
      val blockingSvc: Blocking.Service = bd.get[Blocking.Service]
      def connectionSvc: Connection = bd.get[DataSource].getConnection

      val connectionWithBlocking: ZLayer[Any, Throwable, Has[Connection] with Has[Blocking.Service]] =
        blockingSvc.effectBlocking(connectionSvc)
          .toLayer
          .map(hc => hc ++ Has(blockingSvc))

      val bc: ZLayer[Any, Throwable, BlockingConnection] = connectionWithBlocking
      qzio.provideLayer(bc)
    }

  //  private[getquill] val prefixToConfig: ZLayer[BlockingPrefix, Throwable, BlockingConfig] = {
  //    ZLayer.fromFunctionManyM { (bp: BlockingPrefix) =>
  //      val prefixSvc = bp.get[Prefix]
  //      val blockingSvc = bp.get[Blocking.Service]
  //      val t = blockingSvc.effectBlocking(Has(LoadConfig(prefixSvc.name))).map(conf => conf ++ Has(blockingSvc))
  //      val io: IO[Throwable, BlockingConfig] = t
  //      io
  //    }
  //  }

  private[getquill] def mapGeneric2[From: Tag, To: Tag](mapping: From => To): ZLayer[Has[From] with Blocking, Throwable, Has[To] with Blocking] = {
    ZLayer.fromFunctionManyM { (provision: Has[From] with Blocking) =>
      val from = provision.get[From]
      val blockingSvc = provision.get[Blocking.Service]
      val t = blockingSvc.effectBlocking(Has(mapping(from))).map(conf => conf ++ Has(blockingSvc))
      val io: IO[Throwable, Has[To] with Blocking] = t
      io
    }
  }

  private[getquill] def mapGeneric[From: Tag, To: Tag](mapping: From => To): ZLayer[Has[From] with Blocking, Throwable, Has[To] with Blocking] = {
    ZLayer.fromServicesManyM[From, Blocking.Service, Has[From] with Blocking, Throwable, Has[To] with Blocking](
      (from: From, b: Blocking.Service) => IO(Has(mapping(from)) ++ Has(b))
    )
  }

  val prefixToConfig: ZLayer[BlockingPrefix, Throwable, BlockingConfig] =
    mapGeneric[Prefix, Config]((from: Prefix) => LoadConfig(from.name))

  //val r: ZLayer[Has[Prefix] with Has[Blocking], Nothing, Has[Config]] =
  val r = ZLayer.fromServicesManyM((p: Prefix, b: Blocking.Service) => IO(Has(LoadConfig(p.name)) ++ Has(b)))

  val configToJdbcConfig: ZLayer[BlockingConfig, Throwable, BlockingJdbcContextConfig] =
    mapGeneric[Config, JdbcContextConfig]((from: Config) => JdbcContextConfig(from))

  val jdbcConfigToDataSource: ZLayer[BlockingJdbcContextConfig, Throwable, BlockingDataSource] =
    mapGeneric[JdbcContextConfig, DataSource]((from: JdbcContextConfig) => from.dataSource)

  val dataSourceToConnection: ZLayer[BlockingDataSource, Throwable, BlockingConnection] =
    mapGeneric[DataSource, Connection]((from: DataSource) => from.getConnection)

  //  private[getquill] val configToDsConf = ZLayer.fromFunctionManyM { conf: Config => IO.effect(JdbcContextConfig(conf)) }
  //  private[getquill] val dsConfToDs = ZLayer.fromFunctionManyM { jconf: JdbcContextConfig => IO.effect(jconf.dataSource) }
  //  private[getquill] val dsToConn = ZLayer.fromAcquireReleaseMany(
  //    ZIO.environment[Has[DataSource]]
  //      .mapEffect(e => effectBlocking(e.get.getConnection)).flatten
  //      .refineToOrDie[SQLException]
  //  )(c => defaultRunner.catchAll(RIO(c.close)))
  //
  def fromPrefix[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingPrefix, Throwable, T] =
    qzio.provideLayer(prefixToConfig >>> configToJdbcConfig >>> jdbcConfigToDataSource >>> dataSourceToConnection)
  def fromConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingConfig, Throwable, T] =
    qzio.provideLayer(configToJdbcConfig >>> jdbcConfigToDataSource >>> dataSourceToConnection)
  def fromDsConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingJdbcContextConfig, Throwable, T] =
    qzio.provideLayer(jdbcConfigToDataSource >>> dataSourceToConnection)
  def fromDs[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingDataSource, Throwable, T] =
    qzio.provideLayer(dataSourceToConnection)

}

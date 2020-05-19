package io.getquill.context.zio

import java.sql.{ Array => _, _ }

import io.getquill.{ NamingStrategy, ReturnAction }
import io.getquill.context.StreamingContext
import io.getquill.context.jdbc.JdbcContextSimplified
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.ContextLogger
import javax.sql.DataSource
import zio.Exit.{ Failure, Success }
import zio.{ Chunk, ChunkBuilder, RIO, Task, UIO, ZIO, ZManaged }
import zio.stream.{ Stream, ZStream }

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

  override def prepareSingle(sql: String, prepare: Prepare): Connection => RIO[Connection, PreparedStatement] = super.prepareSingle(sql, prepare)

  override type PrepareRow = PreparedStatement
  override type ResultRow = ResultSet
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]
  override type PrepareQueryResult = RIO[Connection, PrepareRow]
  override type PrepareActionResult = RIO[Connection, PrepareRow]
  override type PrepareBatchActionResult = RIO[Connection, List[PrepareRow]]

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  override def executeAction[T](sql: String, prepare: Prepare = identityPrepare): RIO[Connection, Long] =
    super.executeAction(sql, prepare)
  override def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Connection, List[T]] =
    super.executeQuery(sql, prepare, extractor)
  override def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): RIO[Connection, T] =
    super.executeQuerySingle(sql, prepare, extractor)
  override def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningBehavior: ReturnAction): RIO[Connection, O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)
  override def executeBatchAction(groups: List[BatchGroup]): RIO[Connection, List[Long]] =
    super.executeBatchAction(groups)
  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): RIO[Connection, List[T]] =
    super.executeBatchActionReturning(groups, extractor)
  override def prepareQuery[T](sql: String, prepare: Prepare, extractor: Extractor[T] = identityExtractor): RIO[Connection, PreparedStatement] =
    super.prepareQuery(sql, prepare, extractor)
  override def prepareAction(sql: String, prepare: Prepare): RIO[Connection, PreparedStatement] =
    super.prepareAction(sql, prepare)
  override def prepareBatchAction(groups: List[BatchGroup]): RIO[Connection, List[PreparedStatement]] =
    super.prepareBatchAction(groups)

  override protected val effect: Runner = Runner.default
  import effect._

  /** ZIO Contexts do not managed DB connections so this is a no-op */
  override def close(): Unit = ()

  protected def withConnection[T](f: Connection => Result[T]): Result[T] = throw new IllegalArgumentException("Not Used")
  override protected def withConnectionWrapped[T](f: Connection => T): RIO[Connection, T] =
    RIO.fromFunction(f) // blocking(RIO.fromFunction(f)) ? (import zio.blocking._)

  trait SameThreadExecutionContext extends scala.concurrent.ExecutionContext {
    def submit(runnable: Runnable): Unit =
      runnable.run()
  }

  def transaction[A](f: RIO[Connection, A]): RIO[Connection, A] = {
    def die = Task.die(new IllegalStateException("The task was cancelled in the middle of a transaction."))
    ZIO.environment[Connection].flatMap(conn =>
      f.onInterrupt(die).onExit {
        case Success(_) =>
          UIO(conn.commit())
        case Failure(cause) =>
          // TODO Are we really catching the result of the conn.rollback() Task's exception?
          catchAll(Task(conn.rollback()) *> Task.halt(cause))
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

  def streamQuery[T](fetchSize: Option[Int], sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): ZStream[Connection, Throwable, T] = {
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

    val ret: ZStream[Connection, Throwable, T] =
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

    ret
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

  override private[getquill] def prepareParams(statement: String, prepare: Prepare): RIO[Connection, Seq[String]] = {
    withConnectionWrapped { conn =>
      prepare(conn.prepareStatement(statement))._1.reverse.map(prepareParam)
    }
  }

  def constructPrepareQuery(f: Connection => Result[PreparedStatement]): RIO[Connection, PreparedStatement] =
    ZIO.environment[Connection].flatMap(c => f(c))

  def constructPrepareAction(f: Connection => Result[PreparedStatement]): RIO[Connection, PreparedStatement] =
    ZIO.environment[Connection].flatMap(c => f(c))

  def constructPrepareBatchAction(f: Connection => Result[List[PreparedStatement]]): RIO[Connection, List[PreparedStatement]] =
    ZIO.environment[Connection].flatMap(c => f(c))
}

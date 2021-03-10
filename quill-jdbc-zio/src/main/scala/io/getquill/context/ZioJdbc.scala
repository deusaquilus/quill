package io.getquill.context

import com.typesafe.config.Config
import io.getquill.JdbcContextConfig
import _root_.zio.{ Has, Task, ZIO, ZLayer, ZManaged }
import io.getquill.util.LoadConfig
import izumi.reflect.Tag

import java.io.Closeable
import java.sql.Connection
import javax.sql.DataSource

object ZioJdbc {
  import _root_.zio.blocking._

  /** Describes a single HOCON Jdbc Config block */
  case class Prefix(name: String)

  private[getquill] val defaultRunner = io.getquill.context.zio.Runner.default

  type BlockingConnection = Has[Connection] with Blocking
  type BlockingDataSource = Has[DataSource with Closeable] with Blocking
  type BlockingJdbcContextConfig = Has[JdbcContextConfig] with Blocking
  type BlockingConfig = Has[Config] with Blocking
  type BlockingPrefix = Has[Prefix] with Blocking

  private[getquill] def mapGeneric[From: Tag, To: Tag](
    mapping: From => To
  ): ZLayer[Has[From] with Blocking, Throwable, Has[To] with Blocking] =
    (for {
      from <- ZIO.service[From]
      blocking <- ZIO.service[Blocking.Service]
      result <- blocking.effectBlocking(mapping(from))
    } yield Has.allOf(result, blocking)).toLayerMany

  private[getquill] def mapAutoCloseableGeneric[From: Tag, To <: AutoCloseable: Tag](mapping: From => To): ZLayer[Has[From] with Blocking, Throwable, Has[To] with Blocking] = {
    val managed =
      for {
        fromBlocking <- ZManaged.environment[Has[From] with Blocking]
        from = fromBlocking.get[From]
        blocking = fromBlocking.get[Blocking.Service]
        r <- ZManaged.fromAutoCloseable(Task(mapping(from)))
      } yield Has(r) ++ Has(blocking)
    ZLayer.fromManagedMany(managed)
  }

  object Layers {
    val prefixToConfig: ZLayer[BlockingPrefix, Throwable, BlockingConfig] =
      mapGeneric[Prefix, Config]((from: Prefix) => LoadConfig(from.name))
    val configToJdbcConfig: ZLayer[BlockingConfig, Throwable, BlockingJdbcContextConfig] =
      mapGeneric[Config, JdbcContextConfig]((from: Config) => JdbcContextConfig(from))
    val jdbcConfigToDataSource: ZLayer[BlockingJdbcContextConfig, Throwable, BlockingDataSource] =
      mapAutoCloseableGeneric[JdbcContextConfig, DataSource with Closeable]((from: JdbcContextConfig) => from.dataSource)
    val dataSourceToConnection: ZLayer[BlockingDataSource, Throwable, BlockingConnection] =
      mapAutoCloseableGeneric[DataSource with Closeable, Connection]((from: DataSource) => from.getConnection)
  }

  import Layers._

  def configureFromDsConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingJdbcContextConfig, Throwable, T] =
    qzio.provideLayer(jdbcConfigToDataSource >>> dataSourceToConnection)
  def configureFromDs[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingDataSource, Throwable, T] =
    qzio.provideLayer(dataSourceToConnection)

  implicit class QuillZioExt[T](qzio: ZIO[BlockingConnection, Throwable, T]) {
    /**
     * Allows the user to specify `Has[DataSource]` instead of `Has[Connection]` for a Quill ZIO value i.e.
     * Converts:<br>
     *   `ZIO[BlockingConnection, Throwable, T]` to `ZIO[BlockingDataSource, Throwable, T]` a.k.a.<br>
     *   `ZIO[Has[Connection] with Blocking, Throwable, T]` to `ZIO[Has[DataSource] with Blocking, Throwable, T]` a.k.a.<br>
     */
    def dependOnDs(): ZIO[BlockingDataSource, Throwable, T] = configureFromDs(qzio)

    /**
     * Allows the user to specify JDBC `DataSource` instead of `BlockingConnection` for a Quill ZIO value i.e.
     * Provides a DataSource object which internally brackets `dataSource.getConnection` and `connection.close()`.
     * This effectively converts:<br>
     *   `ZIO[BlockingConnection, Throwable, T]` to `ZIO[Blocking, Throwable, T]` a.k.a.<br>
     *   `ZIO[Has[Connection] with Blocking, Throwable, T]` to `ZIO[Blocking, Throwable, T]` a.k.a.<br>
     */
    def provideDs(ds: DataSource with Closeable): ZIO[Blocking, Throwable, T] =
      provideOne(ds)(qzio.dependOnDs())

    /**
     * Allows the user to specify JDBC `Connection` instead of `BlockingConnection` for a Quill ZIO value i.e.
     * Provides a Connection object which converts:<br>
     *   `ZIO[BlockingConnection, Throwable, T]` to `ZIO[Blocking, Throwable, T]` a.k.a.<br>
     *   `ZIO[Has[Connection] with Blocking, Throwable, T]` to `ZIO[Blocking, Throwable, T]` a.k.a.<br>
     */
    def provideConnection(conn: Connection): ZIO[Blocking, Throwable, T] =
      provideOne(conn)(qzio)
  }

  private[getquill] def provideOne[P: Tag, T, E: Tag, Rest <: Has[_]: Tag](provision: P)(qzio: ZIO[Has[P] with Rest, E, T]): ZIO[Rest, E, T] =
    for {
      rest <- ZIO.environment[Rest]
      env = Has(provision) ++ rest
      result <- qzio.provide(env)
    } yield result
}

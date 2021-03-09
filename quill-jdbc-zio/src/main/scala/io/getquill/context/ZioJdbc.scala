package io.getquill.context

import com.typesafe.config.Config
import io.getquill.JdbcContextConfig
import _root_.zio.{ Has, Task, ZIO, ZLayer, ZManaged }
import zio.{ ZioCatchAll }
import io.getquill.util.LoadConfig
import izumi.reflect.Tag

import java.io.Closeable
import java.sql.Connection
import javax.sql.DataSource

object ZioJdbc extends ZioCatchAll {
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

  private[getquill] def mapBracketGeneric[From: Tag, To: Tag](mapping: From => To)(close: To => Unit): ZLayer[Has[From] with Blocking, Throwable, Has[To] with Blocking] = {
    val managed =
      for {
        fromBlocking <- ZManaged.environment[Has[From] with Blocking]
        from = fromBlocking.get[From]
        blocking = fromBlocking.get[Blocking.Service]
        r <- ZManaged.make(Task(mapping(from)))(to => catchAll(Task(close(to))))
      } yield Has(r) ++ Has(blocking)
    ZLayer.fromManagedMany(managed)
  }

  object Layers {
    val prefixToConfig: ZLayer[BlockingPrefix, Throwable, BlockingConfig] =
      mapGeneric[Prefix, Config]((from: Prefix) => LoadConfig(from.name))
    val configToJdbcConfig: ZLayer[BlockingConfig, Throwable, BlockingJdbcContextConfig] =
      mapGeneric[Config, JdbcContextConfig]((from: Config) => JdbcContextConfig(from))
    val jdbcConfigToDataSource: ZLayer[BlockingJdbcContextConfig, Throwable, BlockingDataSource] =
      mapBracketGeneric[JdbcContextConfig, DataSource with Closeable]((from: JdbcContextConfig) => from.dataSource)(ds => ds.close())
    val dataSourceToConnection: ZLayer[BlockingDataSource, Throwable, BlockingConnection] =
      mapBracketGeneric[DataSource with Closeable, Connection]((from: DataSource) => from.getConnection)(conn => conn.close())
  }

  import Layers._

  def configureFromDsConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingJdbcContextConfig, Throwable, T] =
    qzio.provideLayer(jdbcConfigToDataSource >>> dataSourceToConnection)
  def configureFromDs[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingDataSource, Throwable, T] =
    qzio.provideLayer(dataSourceToConnection)

  // TODO comments
  implicit class QuillZioExt[T](qzio: ZIO[BlockingConnection, Throwable, T]) {
    def dependOnDs(): ZIO[BlockingDataSource, Throwable, T] = configureFromDs(qzio)

    def provideDs(ds: DataSource with Closeable): ZIO[Blocking, Throwable, T] =
      provideOne(ds)(qzio.dependOnDs())
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

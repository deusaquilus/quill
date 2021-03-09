package io.getquill.context

import com.typesafe.config.Config
import io.getquill.JdbcContextConfig
import _root_.zio.{ Has, Task, ZIO, ZLayer }
import io.getquill.context.zio.{ Runner, ZioCatchAll }
import io.getquill.util.LoadConfig
import izumi.reflect.Tag

import java.sql.Connection
import javax.sql.DataSource

object ZioJdbc extends ZioCatchAll {
  import _root_.zio.blocking._

  case class Prefix(name: String)

  private[getquill] val defaultRunner = io.getquill.context.zio.Runner.default

  type BlockingConnection = Has[Connection] with Blocking
  type BlockingDataSource = Has[DataSource] with Blocking
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

  private[getquill] val prefixToConfig: ZLayer[BlockingPrefix, Throwable, BlockingConfig] =
    mapGeneric[Prefix, Config]((from: Prefix) => LoadConfig(from.name))
  private[getquill] val configToJdbcConfig: ZLayer[BlockingConfig, Throwable, BlockingJdbcContextConfig] =
    mapGeneric[Config, JdbcContextConfig]((from: Config) => JdbcContextConfig(from))
  private[getquill] val jdbcConfigToDataSource: ZLayer[BlockingJdbcContextConfig, Throwable, BlockingDataSource] =
    mapGeneric[JdbcContextConfig, DataSource]((from: JdbcContextConfig) => from.dataSource)
  private[getquill] val dataSourceToConnection: ZLayer[BlockingDataSource, Throwable, BlockingConnection] =
    mapGeneric[DataSource, Connection]((from: DataSource) => from.getConnection)

  // Trying to provide self-closing connection layer but so far not working
  // trying to get connection to automatically close afterward but not working
  //  private[getquill] val dataSourceToConnection: ZLayer[BlockingDataSource, Throwable, BlockingConnection] =
  //    (for {
  //      ds <- ZIO.environment[BlockingDataSource]
  //      blocking <- ZIO.service[Blocking.Service]
  //      result <- {
  //        val man = ZManaged.make(blocking.effectBlocking(ds.get[DataSource].getConnection))(conn => Runner.default.catchAll(blocking.effectBlocking(conn.close())))
  //        man.use(conn => Task(conn))
  //      }
  //    } yield Has.allOf(result, blocking)).toLayerMany

  def configureFromPrefix[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingPrefix, Throwable, T] =
    qzio.provideLayer(prefixToConfig >>> configToJdbcConfig >>> jdbcConfigToDataSource >>> dataSourceToConnection)
  def configureFromConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingConfig, Throwable, T] =
    qzio.provideLayer(configToJdbcConfig >>> jdbcConfigToDataSource >>> dataSourceToConnection)
  def configureFromDsConf[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingJdbcContextConfig, Throwable, T] =
    qzio.provideLayer(jdbcConfigToDataSource >>> dataSourceToConnection)
  def configureFromDs[T](qzio: ZIO[BlockingConnection, Throwable, T]): ZIO[BlockingDataSource, Throwable, T] =
    for {
      blockDs <- ZIO.environment[BlockingDataSource]
      ds = blockDs.get[DataSource]
      blocking = blockDs.get[Blocking.Service]
      r <- ZIO.bracket(Task({ val c = ds.getConnection; println("Getting Connection"); c }))(conn => Runner.default.wrapClose({ conn.close(); println("Closing Connection") }))(
        // again, for bracketing to work properly, you have to flatMap the task inside
        conn => Task(conn).flatMap(_ => qzio.provide(Has(conn) ++ Has(blocking)))
      )
    } yield r

  implicit class QuillZioExt[T](qzio: ZIO[BlockingConnection, Throwable, T]) {
    def dependOnPrefix(): ZIO[BlockingPrefix, Throwable, T] = configureFromPrefix(qzio)
    def dependOnConf(): ZIO[BlockingConfig, Throwable, T] = configureFromConf(qzio)
    def dependOnDsConf(): ZIO[BlockingJdbcContextConfig, Throwable, T] = configureFromDsConf(qzio)
    def dependOnDs(): ZIO[BlockingDataSource, Throwable, T] = configureFromDs(qzio)

    def providePrefix(prefix: Prefix): ZIO[Blocking, Throwable, T] =
      provideOne(prefix)(qzio.dependOnPrefix())
    def provideConf(config: Config): ZIO[Blocking, Throwable, T] =
      provideOne(config)(qzio.dependOnConf())
    def provideDsConf(dsConf: JdbcContextConfig): ZIO[Blocking, Throwable, T] =
      provideOne(dsConf)(qzio.dependOnDsConf())
    def provideDs(ds: DataSource): ZIO[Blocking, Throwable, T] =
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

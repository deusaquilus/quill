package io.getquill

import com.zaxxer.hikari.HikariDataSource
import zio._
import io.getquill.context.zio.Runner

object Connector {
  val r = Runner.default
  import r._

  def apply(config: JdbcContextConfig) = {
    RIO.effect(config.dataSource).map(_.getConnection).bracket(
      conn => catchAll(Task(conn.close())),
      conn => Task(conn)
    )
  }

  //  def this(config: Config) = this(naming, JdbcContextConfig(config), runner)
  //  def this(configPrefix: String) = this(naming, LoadConfig(configPrefix), runner)
}

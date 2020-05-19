package io.getquill

import com.typesafe.config.Config
import io.getquill.context.jdbc.PostgresJdbcContextSimplified
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.context.zio.ZioJdbcContext
import io.getquill.util.LoadConfig
import javax.sql.DataSource

class PostgresZioJdbcContext[N <: NamingStrategy](val naming: N)
  extends ZioJdbcContext[PostgresDialect, N]
  with PostgresJdbcContextSimplified[N]

trait WithProbing[D <: SqlIdiom, N <: NamingStrategy] extends ZioJdbcContext[D, N] {
  def probingConfig: Config;
  override def probingDataSource: Option[DataSource] = Some(JdbcContextConfig(probingConfig).dataSource)
}

trait WithProbingPrefix[D <: SqlIdiom, N <: NamingStrategy] extends WithProbing[D, N] {
  def configPrefix: String;
  def probingConfig: Config = LoadConfig(configPrefix)
}

package io.getquill.postgres

import java.util.UUID
import io.getquill.{ JdbcContextConfig, Literal, PostgresZioJdbcContext, TestEntities, ZioSpec }
import io.getquill.context.sql.ProductSpec
import io.getquill.context.zio.ZioJdbcContext
import io.getquill.context.zio.ZioJdbcContext.Prefix
import io.getquill.util.LoadConfig
import io.getquill.context.zio.ZioJdbcContext._
import zio.Runtime

import scala.util.Random

// TODO Need self-closing connection layer for this to work
class ConnectionLeakTest extends ProductSpec with ZioSpec {

  override def prefix: ZioJdbcContext.Prefix = Prefix("testPostgresLeakDB")
  val dataSource = JdbcContextConfig(LoadConfig("testPostgresLeakDB")).dataSource
  val context = new PostgresZioJdbcContext(Literal)
  import context._

  override def beforeAll = {
    context.run(quote(query[Product].delete)).runSyncUnsafe()
    ()
  }

  "insert and select without leaking" in {
    val result =
      Runtime.default.unsafeRun(context.transaction {
        for {
          _ <- context.run {
            quote {
              query[Product].insert(
                lift(Product(1, UUID.randomUUID().toString, Random.nextLong()))
              )
            }
          }
          result <- context.run {
            query[Product].filter(p => query[Product].map(_.id).max.exists(_ == p.id))
          }
        } yield (result)
      }
        .map(_.headOption.map(_.id))
        .provideDs(dataSource))

    Thread.sleep(2000)

    result mustEqual Option(1)
    dataSource.getHikariPoolMXBean.getActiveConnections mustEqual 0

    context.close()
  }

}

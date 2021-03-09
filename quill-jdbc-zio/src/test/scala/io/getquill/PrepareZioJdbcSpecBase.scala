package io.getquill

import io.getquill.ZioTestUtil._
import io.getquill.context.ZioJdbc.{ BlockingConnection, _ }
import io.getquill.context.jdbc.ResultSetExtractor
import io.getquill.context.sql.ProductSpec
import org.scalactic.Equality
import zio.{ RIO, Task, ZIO }

import java.sql.{ PreparedStatement, ResultSet }

trait PrepareZioJdbcSpecBase extends ProductSpec {

  implicit val productEq = new Equality[Product] {
    override def areEqual(a: Product, b: Any): Boolean = b match {
      case Product(_, desc, sku) => desc == a.description && sku == a.sku
      case _                     => false
    }
  }

  def productExtractor: ResultSet => Product

  def withOrderedIds(products: List[Product]) =
    products.zipWithIndex.map { case (product, id) => product.copy(id = id.toLong + 1) }

  def singleInsert(prefix: Prefix)(prep: RIO[BlockingConnection, PreparedStatement]) = {
    prep.providePrefix(prefix).bracket(stmt => catchAll(Task(stmt.close()))) { stmt =>
      Task(stmt.execute())
    }.defaultRun
  }

  def batchInsert(prefix: Prefix)(prep: RIO[BlockingConnection, List[PreparedStatement]]) = {
    prep.providePrefix(prefix).flatMap(stmts =>
      ZIO.collectAll(
        stmts.map(stmt =>
          Task(stmt).bracket(stmt => catchAll(Task(stmt.close()))) { stmt => Task(stmt.execute()) })
      )).defaultRun
  }

  def extractResults[T](prefix: Prefix)(prep: RIO[BlockingConnection, PreparedStatement])(extractor: ResultSet => T) = {
    prep.providePrefix(prefix).bracket(stmt => catchAll(Task(stmt.close()))) { stmt =>
      Task(stmt.executeQuery()).bracket(rs => catchAll(Task(rs.close()))) { rs =>
        Task(ResultSetExtractor(rs, extractor))
      }
    }.defaultRun
  }

  def extractProducts(prefix: Prefix)(prep: RIO[BlockingConnection, PreparedStatement]) =
    extractResults(prefix)(prep)(productExtractor)
}

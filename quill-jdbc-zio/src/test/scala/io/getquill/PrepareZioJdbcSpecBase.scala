package io.getquill

import io.getquill.ZioTestUtil._
import io.getquill.context.ZioJdbc.{ BlockingConnection, _ }
import io.getquill.context.jdbc.ResultSetExtractor
import io.getquill.context.sql.ProductSpec
import org.scalactic.Equality
import zio.{ RIO, Task, ZIO }

import java.sql.{ PreparedStatement, ResultSet }

trait PrepareZioJdbcSpecBase extends ProductSpec with ZioSpec {

  implicit val productEq = new Equality[Product] {
    override def areEqual(a: Product, b: Any): Boolean = b match {
      case Product(_, desc, sku) => desc == a.description && sku == a.sku
      case _                     => false
    }
  }

  def productExtractor: ResultSet => Product

  def withOrderedIds(products: List[Product]) =
    products.zipWithIndex.map { case (product, id) => product.copy(id = id.toLong + 1) }

  def singleInsert(prep: RIO[BlockingConnection, PreparedStatement]) = {
    prep.flatMap(stmt =>
      Task(stmt).bracketAuto { stmt => Task(stmt.execute()) }).provideDs(pool).defaultRun
  }

  def batchInsert(prep: RIO[BlockingConnection, List[PreparedStatement]]) = {
    prep.flatMap(stmts =>
      ZIO.collectAll(
        stmts.map(stmt =>
          Task(stmt).bracketAuto { stmt => Task(stmt.execute()) })
      )).provideDs(pool).defaultRun
  }

  def extractResults[T](prep: RIO[BlockingConnection, PreparedStatement])(extractor: ResultSet => T) = {
    prep.bracketAuto { stmt =>
      Task(stmt.executeQuery()).bracketAuto { rs =>
        Task(ResultSetExtractor(rs, extractor))
      }
    }.provideDs(pool).defaultRun
  }

  def extractProducts(prep: RIO[BlockingConnection, PreparedStatement]) =
    extractResults(prep)(productExtractor)
}

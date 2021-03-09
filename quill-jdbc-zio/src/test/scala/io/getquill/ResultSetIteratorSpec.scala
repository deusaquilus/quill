package io.getquill

import io.getquill.ZioTestUtil._
import io.getquill.context.ZioJdbc._
import io.getquill.util.LoadConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers._
import zio.Task

import scala.collection.mutable.ArrayBuffer

class ResultSetIteratorSpec extends AnyFreeSpec with BeforeAndAfterAll {

  val ds = JdbcContextConfig(LoadConfig("testPostgresDB")).dataSource

  val ctx = new PostgresZioJdbcContext(Literal)
  import ctx._

  case class Person(name: String, age: Int)

  val peopleInsert =
    quote((p: Person) => query[Person].insert(p))

  val peopleEntries = List(
    Person("Alex", 60),
    Person("Bert", 55),
    Person("Cora", 33)
  )

  override def beforeAll = {
    ctx.transaction {
      for {
        _ <- ctx.run(query[Person].delete)
        _ <- ctx.run(liftQuery(peopleEntries).foreach(p => peopleInsert(p)))
      } yield ()
    }.providePrefix(Prefix("testPostgresDB")).defaultRun
  }

  "traverses correctly" in {
    val results =
      Task(ds.getConnection).bracket(conn => catchAll(Task(conn.close()))) { conn =>
        Task {
          val stmt = conn.prepareStatement("select * from person")
          val rs = new ResultSetIterator[String](stmt.executeQuery(), extractor = (rs) => { rs.getString(1) })
          val accum = ArrayBuffer[String]()
          while (rs.hasNext) accum += rs.next()
          accum
        }
      }.defaultRun

    results must contain theSameElementsAs (peopleEntries.map(_.name))
  }

  "can take head element" in {
    val result =
      Task(ds.getConnection).bracket(conn => catchAll(Task(conn.close()))) { conn =>
        Task {
          val stmt = conn.prepareStatement("select * from person where name = 'Alex'")
          val rs = new ResultSetIterator(stmt.executeQuery(), extractor = (rs) => { rs.getString(1) })
          rs.head
        }
      }.defaultRun

    result must equal("Alex")
  }
}

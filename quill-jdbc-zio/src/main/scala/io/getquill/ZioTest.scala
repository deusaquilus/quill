package io.getquill

//import java.sql.SQLException

import java.sql.{Connection, SQLException}
import com.typesafe.config.Config
import io.getquill.context.zio.ZioJdbcContext.Prefix
import io.getquill.util.LoadConfig

import javax.sql.DataSource
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.internal.Platform
//import zio.Exit._

case class Person(name: String, age: Int)

//object MyZioContext extends PostgresZioJdbcContext(Literal)

object ZioTest {

  def main(args: Array[String]): Unit = {
    import io.getquill.context.zio.ZioJdbcContext.Implicits._

    val ctx = new PostgresZioJdbcContext(Literal)
    import ctx._
    val exec: RIO[Has[Connection] with Blocking, List[Person]] = ctx.run(query[Person])

    val provided =
      Prefix("testSqlServerDB").provideFor(exec.configureFromPrefix())

    println(Runtime.default.unsafeRun(provided))


    //r.configureFromPrefix().provide(effectBlocking(Prefix("testSqlServerDB")))






//    val svc: ZLayer[Any, Nothing, Prefix with Blocking] = Prefix.simple ++ zio.blocking.Blocking.live
//
//
//
//    val v = {
//      zio.blocking.blocking {
//        for {
//          prefix <- Prefix.postgres
//          r <- fromPrefix(r).provide(prefix)
//        } yield r
//      }
//    }
//    v.provideLayer(svc)



    //    val prefixEnv = ZIO.environment[String]
//    val prefixDataSource =
//      prefixEnv
//        .mapEffect(LoadConfig(_))
//        .mapEffect(JdbcContextConfig(_))
//        .mapEffect(_.dataSource)
//    val aquired = prefixDataSource.map(_.getConnection)
//    val withBracketedConn = RIO.bracket(aquired)(c => catchAll(RIO(c.close())))(c => RIO.fromFunction((str: String) => c))

    //ZManaged.make()

    //val prefix = ZIO.environment[String]
    import zio.blocking._



    //val blockingService = Blocking.Service.live
    //import blockingService.blocking


    // todo try blocking
    //ZLayer.fromAcquireReleaseMany(dsConfToDs.build.useNow.mapEffect(_.getConnection), )






    //    def die = Task.die(new RuntimeException("Die from interruption"))
    //    def doQuery() = Task({
    //      println("Running Query")
    //      throw new RuntimeException("<Query Died>")
    //    })
    //    def doRollback() = Task(())
    //    def openConnection() = println("Connection Opened")
    //    def closeConnection() = {
    //      println("Connection Close")
    //      //throw new RuntimeException("<Connection Close Error>")
    //    }
    //
    //    val z = Task(println("started")).bracket(
    //      c => URIO(closeConnection()).onExit {
    //        case Exit.Success(_) =>
    //          URIO(println("Close Success"))
    //        case Exit.Failure(cause) =>
    //          URIO(println("Close Failed due to: " + cause))
    //      },
    //      c => URIO(openConnection())
    //    ).flatMap(t => doQuery().onInterrupt(die).onExit {
    //        case Exit.Success(_) =>
    //          URIO(println("Success"))
    //        case Exit.Failure(cause) =>
    //          URIO(println("Failed due to: " + cause))
    //      })
    //
    //    val runtime = Runtime.default
    //    runtime.unsafeRun(z)

    //
    //    Task(println("started")).flatMap(doQuery.onInterrupt(die).onExit {
    //      case Success(_) => UIO(println("yay, succeeded"))
    //      case Failure(cause) =>
    //        ((Task(doRollback) *> Task.halt(cause))).onExit {
    //          case Success(_) => UIO(println("yay, succeeded rollback"))
    //          case Failure(otherCause) => Task.halt(cause ++ otherCause).catchAll {
    //            case _: SQLException              => Task.unit // TODO Log something. Can't have anything in the error channel... still needed
    //            case _: IndexOutOfBoundsException => Task.unit
    //            case e                            => Task.die(e) //: ZIO[Any, Nothing, Unit]
    //          }
    //        }.catchAll {
    //          case _: SQLException              => Task.unit // TODO Log something. Can't have anything in the error channel... still needed
    //          case _: IndexOutOfBoundsException => Task.unit
    //          case e                            => Task.die(e) //: ZIO[Any, Nothing, Unit]
    //        }
    //    })

  }
}

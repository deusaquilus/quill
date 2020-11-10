package io.getquill

//import java.sql.SQLException

import java.sql.{Connection, SQLException}

import com.typesafe.config.Config
import io.getquill.util.LoadConfig
import javax.sql.DataSource
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.internal.Platform
//import zio.Exit._

case class Person(name: String, age: Int)

object ZioTest {

//  type SqlDataSource = Has[SqlDataSource.Service]
//  object SqlDataSource {
//    trait Service {
//      def dataSource: Task[DataSource]
//    }
//    val simple: Layer[Nothing, DataSource] = ZLayer.fromFunction { (ds: DataSource) =>
//      new Service {
//        override def dataSource: Task[DataSource] = Task(ds)
//      }
//    }
//
//    //def postgres = ZIO.accessM { (p: Prefix) => p.get.prefix }
//  }

  def main(args: Array[String]): Unit = {
    val ctx = new PostgresZioJdbcContext(Literal)
    import ctx._
    val runner = io.getquill.context.zio.Runner.default
    import runner._
    val r = ctx.run(query[Person])
    val conf = JdbcContextConfig(LoadConfig("postgres"))
    val print = r.map(r => println(r))
    //val runtime = Runtime.apply(conf.dataSource.getConnection, Platform.default)
    //runtime.unsafeRun(print)


    val prefixToConfig = ZLayer.fromFunctionManyM { (str: String) => IO.effect(LoadConfig(str)) }
    val configToDsConf = ZLayer.fromFunctionManyM { conf: Config => IO.effect(JdbcContextConfig(conf)) }
    val dsConfToDs = ZLayer.fromFunctionManyM { jconf: JdbcContextConfig => IO.effect(jconf.dataSource) }
    val dsToConn = ZLayer.fromAcquireReleaseMany(ZIO.environment[DataSource].mapEffect(_.getConnection).refineToOrDie[SQLException])(c => catchAll(RIO(c.close)))


    def fromPrefixService[T](qzio: ZIO[Connection, Throwable, T]) =
      qzio.provideLayer(prefixToConfig >>> configToDsConf >>> dsConfToDs >>> dsToConn)

    def fromPrefix[T](qzio: ZIO[Connection, Throwable, T]) =
      qzio.provideLayer(prefixToConfig >>> configToDsConf >>> dsConfToDs >>> dsToConn)
    def fromConf[T](qzio: ZIO[Connection, Throwable, T]) =
      qzio.provideLayer(configToDsConf >>> dsConfToDs >>> dsToConn)
    def fromDsConf[T](qzio: ZIO[Connection, Throwable, T]) =
      qzio.provideLayer(dsConfToDs >>> dsToConn)
    def fromDs[T](qzio: ZIO[Connection, Throwable, T]) =
      qzio.provideLayer(dsToConn)


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

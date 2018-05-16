// Copyright (c) 2013-2018 Rob Norris and Contributors
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package example

import cats.effect.{ Async, IO }
import doobie._, doobie.implicits._
import java.sql.ResultSet
import scala.Predef._

object MySqlStreaming {

  // an interpreter that sets up queries for postgres streaming
  object Interp extends KleisliInterpreter[IO] {
    val M = implicitly[Async[IO]]

    override lazy val ConnectionInterpreter =
      new ConnectionInterpreter {
        override def prepareStatement(sql: String) = primitive { c =>
          println("*** prepareStatement")
          val ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          ps.unwrap(classOf[com.mysql.cj.jdbc.StatementImpl]).enableStreamingResults()
          ps
        }
      }

    override lazy val PreparedStatementInterpreter =
      new PreparedStatementInterpreter {
        override def setFetchSize(n: Int) = primitive { ps =>
          println("*** setFetchSize")
          ps.setFetchSize(Int.MinValue) // ignore what was requested
        }
      }

  }

  val baseXa = Transactor.fromDriverManager[IO](
    "com.mysql.cj.jdbc.Driver",
    "jdbc:mysql://localhost:3306/world?useSSL=false&serverTimezone=America/Chicago",
    "root", ""
  )

  // A transactor that uses our interpreter above
  val xa: Transactor[IO] =
    Transactor.interpret.set(baseXa, Interp.ConnectionInterpreter)

  def prog: IO[List[(String, String)]] =
    sql"SELECT c1.xname, c2.xname, c3.xname FROM city c1, city c2, city c3 limit 10000000"
      .query[(String, String)]
      .stream
      .take(10)
      .compile
      .toList
      .transact(xa)

  def main(args: Array[String]): Unit = {
    import System.{ currentTimeMillis => now }
    val t0 = now
    prog.unsafeRunSync.foreach(println)
    println(s"Elapsed: ${now - t0} ms.")
  }

}

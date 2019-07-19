package de.tu_dresden.el2db

import java.sql.Connection

import com.typesafe.scalalogging.StrictLogging
import slick.jdbc.PostgresProfile
import slick.jdbc.PostgresProfile.api._

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class DBParams(dbpath: String, dbname: String, user:String, password: String) {
  def url:String = dbpath + "/" + dbname
}

object DatabaseManager extends StrictLogging {
  type connectionURL = String
  var managers: mutable.Map[DBParams, DatabaseManager] = mutable.Map()
  def getManager(params: DBParams): DatabaseManager = {
    managers.get(params) match {
      case Some(m) => m
      case None => {
        createDatabaseIfNotExists(params)
        //if (!areValidParams(params)) throw new Exception("The given url is not valid for a DB connection.")
        val m = new DatabaseManager(params)
        managers.+=((params, m))
        m
      }
    }
  }

  def createDatabaseIfNotExists(params: DBParams): Boolean = {
    import java.sql.DriverManager
    classOf[org.postgresql.Driver]
    val conn = DriverManager.getConnection(params.dbpath + "/postgres", params.user, params.password)
    try {
      val statement = conn.createStatement()
      logger.info(s"Creating new DB ${params.dbname}")
      statement.execute("CREATE DATABASE " + params.dbname)
    }
    catch {
      case e:org.postgresql.util.PSQLException => {
        e.getSQLState match {
          case "42P04" => return false
          case _ => throw e
        }
      }
    }
    finally {
      // Now close the default DB so that we can connect to the new DB.
      conn.close()
    }
    return true
  }

  def areValidParams(params: DBParams): Boolean = {
    val c = Database.forURL(params.url, params.user, params.password, driver = "org.postgresql.Driver")
    if (c == null) return false
    else {
      c.close()
      return true
    }
  }
}

class DatabaseManager(params: DBParams) extends AutoCloseable with StrictLogging {
  var db: PostgresProfile.backend.Database = Database.forURL(params.url, params.user, params.password, driver = "org.postgresql.Driver")
  // TODO: Connection Pooling?

  val dbparams = params
  val dbname = params.dbname

  def withConnection[A](block: Connection => A): A = {
    val c = db.createSession()
    try {
      val r = block(c.conn)
      return r
    } catch {
      case e:org.postgresql.util.PSQLException=> {
        logger.debug(params.toString)
        logger.debug(e.getServerErrorMessage.toString)
        throw e
      }
    }
    finally {
      c.close()
    }
  }
  def withDatabase[A](block: PostgresProfile.backend.Database => Future[A]): A = {
    Await.result(block(db), Duration.Inf)
  }

  def run[A](action: DBIOAction[A,NoStream,Nothing]): A = {
    Await.result(try {
      db.run(action)
    } catch {
      case e: org.postgresql.util.PSQLException => {
        logger.debug(params.toString)
        logger.debug(e.getServerErrorMessage.toString)
        throw e
      }
    }, Duration.Inf)
  }

  def raw(sql: String): Unit = {
    val s = db.createSession()
    val q = s.prepareStatement(sql).execute()
    s.close()
  }

  def insert(relation: String, values: Iterable[String], conflict: String = "ON CONFLICT DO NOTHING") = {
    if (!values.isEmpty) {
      val query = s"INSERT INTO $relation VALUES ${values.mkString(",")} $conflict"
      logger.debug(query)
      raw(query)
    }
  }


  override def close(): Unit = db.close()
}

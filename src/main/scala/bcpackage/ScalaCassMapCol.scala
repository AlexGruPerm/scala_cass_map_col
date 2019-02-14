package bcpackage

import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

/*
 Immutable Map x.asScala.toMap
 Mutable Map   x.asScala
*/
object ScalaCassMapCol extends App {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  slog.log.debug("BEGIN")
  val node: String = "193.124.112.90"
  val session : Option[Session] = (new CassConnect).getCassSession(node)

  session match {
    case Some(casSess) => {
      try {
        slog.log.debug("Cassandra session exists. Continue.")
        (new BarAsMapsReader(casSess)).run
      } catch {
        case ex: Throwable => {
          slog.log.error(" ------------------------------------------------- ")
          ex.printStackTrace()
          slog.log.error(" ------------------------------------------------- ")
        }
      } finally {
        casSess.close()
      }
    }
    case None  => {
      slog.log.debug("Fail, no cassandra session.")
    }
  }

}

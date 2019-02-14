package bcpackage

import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

class CassConnect {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  def getCassSession(node_address : String) : Option[Session] = {
    slog.log.debug("CassConnect.getCassSession - try open connection to cassandra")
    try {
      val session = Cluster.builder().addContactPoint(node_address).build().connect()
      Option(session)
    } catch {
      case noHostExc: NoHostAvailableException => {
        slog.log.error("Host for connection not available ["+node_address+"]")
        None
      }
      case _: Throwable => {
        slog.log.error("Any kind of exception.")
        None
      }
    }

  }

}

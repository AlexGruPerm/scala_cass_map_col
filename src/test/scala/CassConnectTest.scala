import bcpackage.{CassConnect, CassQueriesBinds}
import com.datastax.driver.core.Session
import org.scalatest.FunSuite

class CassConnectTest extends FunSuite {

  test("CassConnect.getCassSession Correct IP") {
    assert((new CassConnect).getCassSession("193.124.112.90").getOrElse(None) != None)
  }

  test("CassConnect.getCassSession Incorrect IP") {
    assert((new CassConnect).getCassSession("193.124.112.9").getOrElse(None) === None)
  }

  test("CassQueriesBinds created with closed session should throw IllegalArgumentException") {
    val node: String = "193.124.112.90"
    val session : Option[Session] = (new CassConnect).getCassSession(node)
    session match {
      case Some(casSess) => {
        casSess.close()
        intercept[IllegalArgumentException] {
          new CassQueriesBinds(casSess)
        }
      }
    }
  }

  test("CassQueriesBinds created with NULL as session should throw IllegalArgumentException") {
        intercept[IllegalArgumentException] {
          new CassQueriesBinds(null)
        }
  }




}



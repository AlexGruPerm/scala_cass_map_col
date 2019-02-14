import bcpackage.CassConnect
import org.scalatest.FunSuite

class CassConnectTest extends FunSuite {

  test("CassConnect.getCassSession Correct IP") {
    assert((new CassConnect).getCassSession("193.124.112.90").getOrElse(None) != None)
  }

  test("CassConnect.getCassSession Incorrect IP") {
    assert((new CassConnect).getCassSession("193.124.112.9").getOrElse(None) === None)
  }


  /*
  test("BarAsMapsReader wuth empty session parameter should produce IllegalArgumentException ") {
    assertThrows[IllegalArgumentException] {
      new BarAsMapsReader()
    }
  }
  */

}




package bcpackage

import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class BarAsMapsReader(session : Session) {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  def run = {
    //CassQueriesBinds contains all queries to Cassandra and binds.
    val cassQBinds = new CassQueriesBinds(session)

    val instSeqOfBars = new SeqOfBars(cassQBinds,slog.log)
    instSeqOfBars.showBarsLog

    /**
      * Now we can take one random bar_x from presented and save it into cassandra.
      */
    val b_tickerId: Int = instSeqOfBars.bars(0).ticker_id
    val bOneName: String = "bar_1"
    val b: Map[String, String] = instSeqOfBars.bars(0).getBarByName(bOneName)
    slog.log.debug("b_tickerId = " + b_tickerId + " (" + bOneName + ") =" + b)

    val querySaveCountTotal =
      """ insert into mts_bars.td_bars_3600(ticker_id,ddate,  bar_1,bar_2,bar_3,bar_4)
                                   values(?,?,              ?,?,?,?) """
    val pquerySaveCountTotal = session.prepare(querySaveCountTotal)

    val boundSaveCountTotal = pquerySaveCountTotal.bind()
      .setInt("ticker_id", instSeqOfBars.bars(0).ticker_id)
      .setDate("ddate", instSeqOfBars.bars(0).ddate)
      .setMap("bar_1", instSeqOfBars.bars(0).getBarByName("bar_1").asJava)
      .setMap("bar_2", instSeqOfBars.bars(0).getBarByName("bar_1").asJava)
      .setMap("bar_3", b.asJava)
      .setMap("bar_4", b.asJava)

    val rsBar = session.execute(boundSaveCountTotal).one()

    /*
    slog.log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    val bc = instSeqOfBars.bars(1).getBarFullByName("bar_1").barc
    slog.log.debug("barc.ts_begin = " + bc.ts_begin + "  o=" + bc.o + "  c=" + bc.c + " btype=" + bc.btype)
    slog.log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
*/

    session.close()

  }
}


object BarAsMapsReader {
  def apply(s: Session) = {
    s match {
      case s:Session => new BarAsMapsReader(s)
      case _ => {
        println("object BarAsMapsReader - throw IllegalArgumentException >>>>>>>>>>>>>>>>>")
        throw new IllegalArgumentException("Invalid input parameter - session, for class BarAsMapsReader")
      }
    }
  }
}
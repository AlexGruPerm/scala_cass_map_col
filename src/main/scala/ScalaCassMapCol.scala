import java.util.Date

import com.datastax.driver.core.{Cluster, Row}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

object ScalaCassMapCol extends App {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  case class bars(ticker_id :Int,
                  ddate     :java.util.Date,
                  tdBars    :Map[String,String]){
    override def toString = "ticker_id="+ddate+"  ["+ddate+"]" + tdBars.toString()
  }



  val rowToBars = (row : Row) => {
    new bars(
      row.getInt("ticker_id"),
      new Date(row.getDate("ddate").getMillisSinceEpoch),
      row.getMap("bar_1", classOf[String], classOf[String]).asScala.toMap
    )
  }

  /*
   Immutable Map myJavaMap.asScala.toMap
   Mutable Map myJavaMap.asScala
  */

  slog.log.debug("BEGIN")
  val node: String = "193.124.112.90"
  private val cluster = Cluster.builder().addContactPoint(node).build()
  val session = cluster.connect()

  /*EXAMPLE OF SELECT.*/

  val queryTicksCountTotal =
    """ select ticker_id,
               ddate,
               bar_1
          from mts_bars.td_bars_3600 """

  val pqueryTicksCountTotal = session.prepare(queryTicksCountTotal)

  val bound = pqueryTicksCountTotal.bind()

  val dsTicksCntTotalRow : Seq[bars] = JavaConverters.asScalaIteratorConverter(session.execute(bound).all().iterator())
    .asScala
    .toSeq.map(rowToBars)
    .sortBy(ft => ft.ticker_id)(Ordering[Int].reverse)

  slog.log.debug("=====================================")
  for (rowTicksCntTotalRow <- dsTicksCntTotalRow){
    slog.log.debug(rowTicksCntTotalRow.toString)
  }
  slog.log.debug("=====================================")

  session.close()

}

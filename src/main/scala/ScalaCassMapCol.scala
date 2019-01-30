import com.datastax.driver.core.{Cluster, Row}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

object ScalaCassMapCol extends App {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  case class TicksCntTotalRow(ticker_id : Int, ticks_count :Long){
    override def toString = "ticker_id="+ticker_id+"  ["+ticks_count+"]"
  }

  val rowToTicksCntTotalRow = (row : Row) => {
    new TicksCntTotalRow(
      row.getInt("ticker_id"),
      row.getLong("ticks_count")
    )
  }

  slog.log.debug("BEGIN")
  val node: String = "193.124.112.90"
  private val cluster = Cluster.builder().addContactPoint(node).build()
  val session = cluster.connect()

  /*EXAMPLE OF SELECT.*/

  val queryTicksCountTotal =
    """ select *
          from mts_src.ticks_count_total """

  val pqueryTicksCountTotal = session.prepare(queryTicksCountTotal)

  val bound = pqueryTicksCountTotal.bind()

  val dsTicksCntTotalRow : Seq[TicksCntTotalRow] = JavaConverters.asScalaIteratorConverter(session.execute(bound).all().iterator())
    .asScala
    .toSeq.map(rowToTicksCntTotalRow)
    .sortBy(ft => ft.ticks_count)(Ordering[Long].reverse)

  for (rowTicksCntTotalRow <- dsTicksCntTotalRow){
    slog.log.debug(rowTicksCntTotalRow.toString)
  }
  session.close()


}

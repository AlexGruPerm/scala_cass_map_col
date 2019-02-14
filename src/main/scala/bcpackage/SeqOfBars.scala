package bcpackage

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class SeqOfBars(cassQBinds : CassQueriesBinds, slog : org.slf4j.Logger){

  def getBarsFromCassandra :Seq[ROW_BARS] = {
    //Get meta information about columns in this DataSet. Column names by pattern bar_x (where x from 1 to N) for MAP fields.
    val colDef = cassQBinds.sess.execute(cassQBinds.bndBars3600).getColumnDefinitions
    val colsBarsNames: Seq[String] = for (thisColumn <- colDef.asList().asScala if thisColumn.getName().substring(0, 3) == "bar")
      yield thisColumn.getName()

    // convRow contains function(s) to convert Cassandra rows into objects.
    val convRow = new converterRows

    JavaConverters.asScalaIteratorConverter(cassQBinds.sess.execute(cassQBinds.bndBars3600).all().iterator())
      .asScala
      .toSeq.map(convRow.rowToBars(_, colsBarsNames))
      .sortBy(ft => ft.ticker_id)(Ordering[Int].reverse)
  }

  val bars :Seq[ROW_BARS] = getBarsFromCassandra

  //Just for visual logging
  def showBarsLog ={
    slog.debug("===================================================================")
    slog.debug("ROWS SIZE=" + bars.size)
    slog.debug(" ")
    for (rowTicksCntTotalRow <- bars) {
      slog.debug("ticker_id = " + rowTicksCntTotalRow.ticker_id + " for [" + rowTicksCntTotalRow.ddate + "]")
      slog.debug("  BARS COUNT = " + rowTicksCntTotalRow.tdBars.size)
      slog.debug("    BARS WITH DATA = " + rowTicksCntTotalRow.tdBars.filter(p => (p.barProp.nonEmpty)).size)
      //local loop by nonEmpty bars.
      for (neBar <- rowTicksCntTotalRow.tdBars.filter(p => (p.barProp.nonEmpty))) {
        slog.debug("      (" + neBar.barName + ") = " + neBar.barProp)
      }
      slog.debug(" ")
    }
    slog.debug("===================================================================")
  }

}

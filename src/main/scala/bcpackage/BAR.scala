package bcpackage

import com.datastax.driver.core.{LocalDate, Row}
import scala.collection.JavaConverters._
//import com.datastax.driver.core.LocalDate

class converterRows{
  /**
    * Function to convert one row of DataSet into object bar
    */
  val rowToBars = (row : Row, colNames : Seq[String]) => {
    new ROW_BARS(
      row.getInt("ticker_id"),
      row.getDate("ddate"),
      for (cn <- colNames) yield
        new BAR(row.getInt("ticker_id"),
          row.getDate("ddate"),
          cn,
          row.getMap(cn, classOf[String], classOf[String]).asScala.toMap)
    )
  }
}

case class BARCORE(ticker_id :Int,
                   ddate     :LocalDate,
                   barProp :Map[String,String]){

  def simpleRound4Double(valueD : Double) = {
    (valueD * 10000).round / 10000.toDouble
  }

  def simpleRound5Double(valueD : Double) = {
    (valueD * 100000).round / 100000.toDouble
  }

  def simpleRound6Double(valueD : Double) = {
    (valueD * 1000000).round / 1000000.toDouble
  }

  val ts_begin        :Long = barProp.getOrElse("ts_begin","0").toLong
  val ts_end          :Long = barProp.getOrElse("ts_end","0").toLong
  val bar_width_sec   :Long   = ts_end - ts_begin
  val o               :Double = simpleRound5Double(barProp.getOrElse("o","0").toDouble)
  val h               :Double = simpleRound5Double(barProp.getOrElse("h","0").toDouble)
  val l               :Double = simpleRound5Double(barProp.getOrElse("l","0").toDouble)
  val c               :Double = simpleRound5Double(barProp.getOrElse("c","0").toDouble)
  val h_body          :Double = simpleRound5Double(math.abs(c-o))
  val h_shad          :Double = simpleRound5Double(math.abs(h-l))
  val btype           :String =(o compare c).signum match {
    case -1 => "g" // bOpen < bClose
    case  0 => "n" // bOpen = bClose
    case  1 => "r" // bOpen > bClose
  }

  val ticks_cnt       :Long =  barProp.getOrElse("ticks_cnt","0").toLong
  val log_co          :Double = simpleRound5Double(Math.log(c)-Math.log(o))
}

/**
  *
  * @param barName - name of bar (Ex: bar_1,bar_2,... also it's a column names in Cassandra mts_bars.td_bars_XXX tables
  * @param barProp - all properties of the BAR, like o,h,l,c,ts_begin,ts_end and etc.
  */
case class BAR(ticker_id :Int,
               ddate     :LocalDate,
               barName   :String,
               barProp   :Map[String,String]){

  val barc = BARCORE(ticker_id,
                     ddate,
                     barProp)

}


/**
  * Class for all bars of one row (ticker_id,ddate,Seq(BAR)) Where BAR has member barc
  * that contains parsed properties like, ts_begin,ts_end,o,h,l,c...
  */
case class ROW_BARS(ticker_id :Int,
                    ddate     :LocalDate,
                    tdBars    :Seq[BAR]){

  override def toString = "ticker_id="+ddate+"  ["+ddate+"]" + tdBars.toString()

  def getBarByName(fbarName : String) = {
    tdBars.filter(p => (p.barName == fbarName)).headOption.get.barProp
  }

  def getBarFullByName(fbarName : String) = {
    tdBars.filter(p => (p.barName == fbarName)).headOption.get
  }



}

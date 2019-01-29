import org.slf4j.LoggerFactory

object ScalaCassMapCol extends App {

  object slog extends Serializable {
    @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
  }

  slog.log.info("BEGIN [CassToHdfs]")

}

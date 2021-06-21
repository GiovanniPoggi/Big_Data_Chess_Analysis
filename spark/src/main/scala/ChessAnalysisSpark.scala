import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Istanzio ed eseguo il primo Job che effettua il conteggio delle partite vinte, perse e pareggiate per
 * ogni tipologia di Apertura degli Scacchi.
 *
 * @author Poggi Giovanni
 */
object ChessAnalysisSpark {
  def main(args: Array[String]): Unit = {
    //Le proprietà Spark controllano la maggior parte delle impostazioni dell'applicazione e sono configurate separatamente per ogni applicazione.
    val conf = new SparkConf().setAppName("Chess Analysis")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Configuro i file di input e output.
    val filePath = if (args.length > 0) args(0) else "hdfs:/user/gpoggi/chessanalisys/chess_games.csv"
    val outputPath = if (args.length > 1) args(1) else "hdfs:/user/gpoggi/sparkstageout/chess_games_output.csv"

    //Imposto i titoli delle colonne del file chessgames.csv
    val fileHeader = Array("Event", "White", "Black", "Result", "UTCDate", "UTCTime", "WhiteElo", "BlackElo", "WhiteRatingDiff",
      "BlackRatingDiff", "ECO", "Opening", "TimeControl", "Termination", "AN")

    //Creo l'headermap.
    val headerMap = fileHeader.zipWithIndex.toMap

    //ChessRdd è una raccolta immutabile di oggetti e lo stiamo creando per effettuare l'elaborazione.
    val chessRdd = sc.parallelize(Seq(filePath))
    val openingRdd = chessRdd.flatMap(file => {
      val rows = new CSVSource(file, new CSVSourceOptions()).iterator
      rows.map(r => {
        val result = r(headerMap("Result")).trim
        val opening = r(headerMap("Opening"))
        val winCount: Long = if (result == "1-0") 1 else 0
        val lossCount: Long = if (result == "0-1") 1 else 0
        val drawCount: Long = if (result == "1/2-1/2") 1 else 0
        val totalCount: Long = 1
        (opening, CountMetrics(winCount, lossCount, drawCount, totalCount))
      })
    })

    //La trasformazione Spark RDD reduceByKey() viene utilizzata per unire i valori di ciascuna chiave utilizzando
    //una funzione di riduzione associativa.
    val openingAgg = openingRdd.reduceByKey(reduceCountMetrics).collect()

    //Genero il risultato finale rimappandolo nel formato migliore
    val result = openingAgg.map(x => Array(x._1, x._2.total, x._2.wins, x._2.draws, x._2.losses))

    //Riscrivo i record sul file .csv
    val csvWriter = new CSVRecordWriter(FileSystem.get(new Configuration()).create(new Path(outputPath)), textQualifier = "\"")
    result.foreach(x => csvWriter.append(x))
    csvWriter.close()
    if (!sc.isStopped) sc.stop()
  }

  private def reduceCountMetrics(a: CountMetrics, b: CountMetrics): CountMetrics = {
    a.add(b)
  }

  //Effettuo il calcolo del totale delle partite vinte, perse e pareggiate per ogni record.
  case class CountMetrics(var wins: Long, var losses: Long, var draws: Long, var total: Long) {
    def add(k: CountMetrics): CountMetrics = {
      this.wins += k.wins
      this.losses += k.losses
      this.draws += k.draws
      this.total += k.total
      this
    }

    lazy val winPercent: Double = (1D * wins / total) * 100

    var globalUsedPct: Double = _

    def globalUsedPercent(totalGames: Long): CountMetrics = {
      globalUsedPct = (1D * total / totalGames) * 100
      this
    }
  }

  class CSVSourceOptions(val columnCount: Option[Int] = None, val skipHeader: Boolean = true, val fieldDelimiter: Char = ',',
                         val textQuote: Char = '"', val format: String = "UTF-8", val isCompressed: Boolean = false) {
  }

  //Analizzo il file riga per riga
  class CSVSource(path: String, options: CSVSourceOptions) {
    private val fs = FileSystem.get(new Configuration())
    val settings = new CsvParserSettings
    settings.getFormat.setDelimiter(options.fieldDelimiter)
    settings.getFormat.setQuote(options.textQuote)
    settings.setMaxCharsPerColumn(8000)
    val parser = new CsvParser(settings)

    private val inpFile = fs.open(new Path(path))
    private val unzipData = if (options.isCompressed) new GZIPInputStream(inpFile) else inpFile
    val is = new BufferedReader(new InputStreamReader(unzipData, options.format))

    parser.beginParsing(is)
    if (options.skipHeader) {
      val lLen = parser.parseNext.length
      if (options.columnCount.nonEmpty && lLen != options.columnCount.get)
        throw new Exception(s"File format is not compatible. Expecting ${options.columnCount} columns but got $lLen")
    }

    //Iteratore con funzionalità base per scorrere il file per righe
    def iterator: Iterator[Array[String]] = {
      new Iterator[Array[String]] {
        var line: Array[String] = parser.parseNext

        override def hasNext: Boolean = {
          line != null
        }

        override def next(): Array[String] = {
          val l = line.map(x => if (x == null) "" else x.trim)
          line = parser.parseNext
          l
        }
      }
    }
  }

  //Scrivo i dati su file per ogni record.
  class CSVRecordWriter(out: org.apache.hadoop.fs.FSDataOutputStream, delimiter: String = ",", textQualifier: String = "") {
    def append(row: Array[Any]) {
      var i = 0
      for (record <- row) {
        if (i != 0) out.writeBytes(delimiter)
        out.writeBytes(if (record == null) "" else s"$textQualifier$record$textQualifier")
        i += 1
      }
      out.writeBytes("\n")
    }

    def close() {
      out.close()
    }
  }
}

import ChessAnalysisSpark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Istanzio ed eseguo il secondo job che utilizza il file di output intermedio creato dal primo Job per elaborarlo ulteriormente.
 * Le percentuali sono calcolate e si basano sull'output del primo Job.
 *
 * @author Poggi Giovanni
 */
object ChessAnalysisSpark2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Chess Analysis1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Se non riconosco gli input o non sono disponibili, utilizzo valori standard.
    //Questi valori vengono modificati prima dell'esecuzione in base all'ambiente di esecuzione.
    val filePath = if (args.length > 0) args(0) else "hdfs:/user/gpoggi/sparkstageout/chess_games_output.csv"
    val outputPath = if (args.length > 1) args(1) else "hdfs:/user/gpoggi/sparkoutputchess/chess_games_output_final.csv"

    //Titoli delle colonne del file letto.
    val fileHeader = Array("opening", "total", "wins", "draws", "losses")
    val headerMap = fileHeader.zipWithIndex.toMap

    //Creo l'RDD che raccoglie gli elementi partizionati tra i nodi del cluster (se disponibili)
    //in modo da poter eseguire varie operazioni parallele su di esso.
    val chessRdd = sc.parallelize(Seq(filePath))

    //L'input è chessRdd e lo filtra per creare un openingRdd.
    val openingRdd = chessRdd.flatMap(file => {
      val rows = new CSVSource(file, new CSVSourceOptions(skipHeader = false, format = "UTF-8")).iterator
      rows.map(r => {
        val opening = r(headerMap("opening"))
        val winCount: Long = r(headerMap("wins")).toLong
        val lossCount: Long = r(headerMap("losses")).toLong
        val drawCount: Long = r(headerMap("draws")).toLong
        val totalCount: Long = r(headerMap("total")).toLong
        (opening, CountMetrics(winCount, lossCount, drawCount, totalCount))
      })
    })

    //Numero totale di partite giocate.
    val totalPlays = openingRdd.map(_._2.total).reduce(_ + _)
    println(s"Total plays: $totalPlays")

    //Dati aggregati calcolati ordinando la percentuale ottenuta.
    val openingAgg = openingRdd.map(x => (x._1, x._2.globalUsedPercent(totalPlays))).collect().sortBy(x => (-x._2.winPercent, -x._2.globalUsedPct, x._1))

    //Genero il risultato finale
    val result = openingAgg.map(x => Array(x._1, x._2.total, x._2.wins, x._2.draws, x._2.losses, f"${x._2.globalUsedPct}%1.2f", f"${x._2.winPercent}%1.2f"))

    //Riscrivo i record sul file .csv
    val csvWriter = new CSVRecordWriter(FileSystem.get(new Configuration()).create(new Path(outputPath)), textQualifier = "\"")
    val outputHeader: Array[Any] = Array("opening", "total_times_used", "win_count", "draw_count", "loss_count",
      "total_times_used_to_total_games_percent", "win_count_to_total_times_used_percent")
    csvWriter.append(outputHeader)
    result.foreach(x => csvWriter.append(x))
    csvWriter.close()
    if (!sc.isStopped) sc.stop()
  }
}
//Eseguo il job che chiamerà le due classi scala per eseguire i jobs di aggregating e sorting.
object ChessAnalysisSparkJobs {
  def main(args: Array[String]): Unit = {
    //Prendo il file iniziale su cui effettuare il primo Job
    val filePath = if (args.length > 0) args(0) else "hdfs:/user/gpoggi/chessanalisys/chess_games.csv"
    //File intermedio che passerò al secondo Job
    val intermediatePath = if (args.length > 1) args(1) else "hdfs:/user/gpoggi/sparkstageout/chess_games_output.csv"
    //Output finale del file .csv con i risultati
    val outputPath = if (args.length > 2) args(1) else "hdfs:/user/gpoggi/sparkoutputchess/chess_games_output_final.csv"

    //Eseguo il primo Job
    ChessAnalysisSpark.main(Array(filePath, intermediatePath))
    //Eseguo il secondo Job
    ChessAnalysisSpark2.main(Array(intermediatePath, outputPath))
  }
}
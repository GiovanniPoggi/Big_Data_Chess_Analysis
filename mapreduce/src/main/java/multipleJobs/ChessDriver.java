package multipleJobs;

import org.apache.hadoop.conf.Configured; //Provides access to configuration parameters.
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The ChessDriver class is responsible for setting our MapReduce job to run in Hadoop.
 * In this class, we specify job name, data type of input/output and names of mapper and reducer classes.
 *
 * @author Poggi Giovanni
 */
public class ChessDriver extends Configured {

	public static void main(String[] args) throws Exception {
		String inputDir = null;
		String stagingDir = null;
		String outputDir = null;
		String finalDir = null;
		long time = System.currentTimeMillis();

		//Effettua un Check degli argomenti passati dalla console per l'esecuzione dell'applicazione
		if (4 > args.length) {
			System.out.print("Usage: Provide <input dir>, <staging dir>, <final_dir>\n");
			//Cartella di Input dov'è situato il file .csv
			inputDir = "hdfs:/user/gpoggi/chessanalysis";
			//Cartella di appoggio in cui salvo temporaneamente il file per mostrare i calcoli intermedi
			stagingDir = "hdfs:/user/gpoggi/staging_t" + time;
			//Cartella di appoggio in cui salvo temporaneamente il file per mostrare i calcoli intermedi
			outputDir = "hdfs:/user/gpoggi/chessanalysis_output_t" + time;
			//Cartella di output finale
			finalDir = "hdfs:/user/gpoggi/chessanalysis_final_t" + time;
		} else {
			inputDir = args[0];
			stagingDir = args[1] + time;
			outputDir = args[2] + time;
			finalDir = args[3] + time;
		}

		//Output riguardo le varie directory su cui ho e sto lavorando
		System.out.println("Arguments =" + args.length);
		System.out.println("inputDir =" + inputDir);
		System.out.println("stagingDir =" + stagingDir);
		System.out.println("outputDir =" + outputDir);
		System.out.println("finalDir =" + finalDir);

		//Crea il Job
		Job job = new Job();

		//Imposto il numero di Reducer come Personalizzabili
		if (args.length == 5) {
			job.setNumReduceTasks(Integer.parseInt(args[4]));
		}

		job.setJarByClass(ChessDriver.class);
		//I file vengono gestiti dai formati di input e output di hadoop in base al nome della directory di input e output
		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(stagingDir));
		//Imposto la classe che gestirà la funzionalità del Mapper
		job.setMapperClass(ChessMapper.class);
		//Imposto la classe che gestirà la funzionalità del Reducer
		job.setReducerClass(ChessReducer.class);

		//Imposto il tipo della classe delle chiavi come testuali, essendo formate dalle "aperture" degli scacchi
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//Imposto il separatore tipico dei formati csv
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		//Attendo che porti a termine il Job
		job.waitForCompletion(true);

		//Eseguo il secondo Job per calcolare la prima percentuale da calcolare
		String[] job2Args = {stagingDir, outputDir};
		ChessAnalysisSort.main(job2Args);

		//Eseguo il terzo Job per calcolare la seconda percentuale da calcolare
		String[] job3Args = {outputDir, finalDir};
		ChessAnalysisPercent.main(job3Args);

		//Output riguardo le varie directory su cui ho e sto lavorando
		System.out.println("inputDir =" + inputDir);
		System.out.println("Staging Directory =" + stagingDir);
		System.out.println("Output Directory =" + outputDir);
		System.out.println("FinalDir Directory =" + finalDir);
	}
}
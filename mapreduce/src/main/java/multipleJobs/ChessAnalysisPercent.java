package multipleJobs;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessAnalysisPercent {

	/**
	 * This is a mapper class that verify the input.
	 *
	 * @author Poggi Giovanni
	 *
	 */
	public static class ChessAnalysisPercentMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (null != value) {
				String str = value.toString();
				String[] data = str.split(",");
				int length = data.length;
				//Verifica se sono presenti 6 token per ogni riga/record, elaborando solo i dati correttamente inseriti
				if (6 == length) {
					context.write(new Text("Percentage"), new Text(str));
				}
			}
		}
	}

	/**
	 * This is a reducer class that consumes all records of the file and it calculate the new percentage.
	 * Percentage is calculated in based of the number of games played to number of games altogether.
	 *
	 * @author Poggi Giovanni
	 */
	public static class ChessAnalysisPercentReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> valuesList = new ArrayList<>();
			int total = 0;
			//Calcolo il totale delle partite giocate
			for (Text value : values) {
				valuesList.add(value.toString());
				String str = value.toString();
				String[] data = str.split(",");
				total = total + Integer.parseInt(data[1]);
			}
			DecimalFormat df = new DecimalFormat("0.000000");
			//Calcolo la nuova percentuale e poi riscrivo il file con tutti i dati che avevamo e che ci servivano
			for (String line : valuesList) {
				String[] data = line.split(",");
				int totalOpenings = Integer.parseInt(data[1]);
				String percent = df.format((totalOpenings * 100L) / total);
				String opening = data[0];
				String gamesPlayed = data[1];
				String won = data[2];
				String lost = data[3];
				String tie = data[4];
				String gpercent = data[5];
				context.write(new Text(opening), new Text(gamesPlayed + "," + won + "," + lost + "," + tie + "," + percent + "," + gpercent));
			}
		}
	}

	/**
	 * This Class is invoked from ChessDriver class. It creates a MapReduce Job by assigning the class Mapper
	 * and the class Reducer. The input file is the output of the second job. The folders are generated
	 * dynamically for this job.
	 *
	 * @author Poggi Giovanni
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Total Percent Calculator");

		//Imposto il numero di Reducer a 1
		job.setNumReduceTasks(1);

		job.setJarByClass(ChessAnalysisPercent.class);
		//Imposto la classe che gestirà la funzionalità del Mapper
		job.setMapperClass(ChessAnalysisPercentMapper.class);
		//Imposto la classe che gestirà la funzionalità del Reducer
		job.setReducerClass(ChessAnalysisPercentReducer.class);

		//Imposto il tipo della classe delle chiavi come testuali
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Imposto il separatore tipico dei formati csv
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
		//Attendo che porti a termine il Job
		job.waitForCompletion(true);
	}
}
package multipleJobs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessAnalysisSort {

	/**
	 * This is the mapper class that consumes records generated from the first job, creates a new column for percentage.
	 * Percentage is calculated in based on numbers of wins of total games played for this opening.
	 *
	 * @author Poggi Giovanni
	 */
	public static class ChessAnalysisSortMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//Check if value exist
			if (null != value) {
				String str = value.toString();
				String[] data = str.split(",");
				int length = data.length;
				//Verifica se sono presenti 5 token per ogni riga/record, elaborando solo i dati correttamente inseriti
				if (5 == length) {
					int total = Integer.parseInt(data[1]);
					int won = Integer.parseInt(data[2]);
					//Calcola la percentuale in base alle vittorie ed il totale
					double percent = (won*100)/total;
					//Scrivo i valori delle chiavi per il Reducer
					context.write(new Text(percent+""), new Text(str));
				}
			}
		}
	}

	/**
	 * This class reduce the list of records and write to output.
	 *
	 * @author Poggi Giovanni
	 */
	public static class ChessAnalysisSortReducer extends Reducer<Text, Text, Text, Text> {
		//Riscrivere i record sul file scambiando la chiave e il valore
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}

	/**
	 * This Class is invoked from ChessDriver class. It creates a MapReduce Job by assigning the class Mapper
	 * and the class Reducer for sorting. The input file is the output of the first job. The folders are generated
	 * dynamically for this job.
	 *
	 * @author Poggi Giovanni
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Total Win Percent Calculator");

		//Imposto il numero di Reducer a 1
		job.setNumReduceTasks(1);

		job.setJarByClass(ChessAnalysisSort.class);
		//Imposto la classe che gestirà la funzionalità del Mapper
		job.setMapperClass(ChessAnalysisSortMapper.class);
		//Imposto la classe che gestirà la funzionalità del Reducer
		job.setReducerClass(ChessAnalysisSortReducer.class);
		//Definisce il comparatore che controlla come vengono ordinate le chiavi prima che vengano passate al Reducer.
		job.setSortComparatorClass(ReverseComparator.class);
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

	/**
	 * This class define the comparator that controls how the keys are sorted before they are passed to the Reducer.
	 *
	 * @author Poggi Giovanni
	 */
	public static class ReverseComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		public ReverseComparator() {
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a instanceof Text && b instanceof Text) {
				return (-1)*(((Text) a).compareTo((Text) b));
			}
			return super.compare(a, b);
		}
	}
}
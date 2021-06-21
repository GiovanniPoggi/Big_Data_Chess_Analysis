package multipleJobs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class registered as job. It is called after mapper job is performed successfully.
 * The results are written as openings, results are further processed in the reducer.
 *
 * @author Poggi Giovanni
 */
public class ChessReducer extends Reducer<Text, Text, Text, Text> {

	//Dal mapper ricevo delle liste e per ogni "apertura" calcolo il numero dei record, alla fine dell'elaborazione
	//il risultato ottenuto è: "apertura", numero di partite, partite vinte, partite perse e partite pareggiate
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int won = 0;
		int lost = 0;
		int tie = 0;
		while (values.iterator().hasNext()) {
			Text val = values.iterator().next();
			String result = val.toString(); 
			if ("0-1".equals(result))
				lost = lost + 1;
			else if ("1-0".equals(result))
				won = won + 1;
			 else 
				tie = tie + 1; 
		}
		int totalPlayed = won + lost + tie;
		String finalVal = totalPlayed + "," + won + "," + lost + "," + tie;
		context.write(key, new Text(finalVal));
	}
}
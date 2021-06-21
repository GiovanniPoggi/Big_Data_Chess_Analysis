package multipleJobs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class is registered as Mapper function while defining the job.
 *
 * @author Poggi Giovanni
 */
public class ChessMapper extends Mapper<LongWritable, Text, Text, Text> {

	//Tokenizza ogni riga con virgole separate e genera dei token. Convalida l'operazione se la riga
	//possiede i dati corretti e prende la 12° colonna (apertura) e la 4° colonna (risultato)
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (null != value) {
			String str = value.toString();
			String[] data = str.split(",");
			int length = data.length;
			//Mi assicuro che la riga abbia tutte le colonne che mi servono per effettuare le operazioni
			if (12 < length) {
				//Prendo la colonna delle aperture e la utilizzo come chiave per l'elaborazione dei dati
				String team = data[11].trim();
				team = team.replace("\"", "");
				//Il risultato sarà valore chiave (opening) e risultato partita per un'ulteriore elaborazione
				context.write(new Text(team), new Text(data[3].trim()));
			}
		}
	}
}
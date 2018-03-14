package simpledb.file;

import java.util.HashMap;
import java.util.Map;

/**
 * CLASSE PER ESERCIZIO 1.1: 
 * Classe che memorizza le statistiche. Memorizza il numero di blocchi letti e scritti,
 * suddividendoli per nome del file.
 * @author micheletedesco1
 *
 */
public class BlockStats {

	private Map<String,Integer> file2blockLetti;
	private Map<String,Integer> file2blockScritti;

	public BlockStats() {
		this.file2blockLetti = new HashMap<>();
		this.file2blockScritti = new HashMap<>();
	}

	public void logReadBlock(Block blk) {
		String filename = blk.fileName();
		Integer count = file2blockLetti.get(filename);
		if (count == null) {
			count = 0;
		}
		else
			count = count + 1;
		file2blockLetti.put(filename, count);

	}

	public void logWriteBlock(Block blk) {
		String filename = blk.fileName();
		Integer count = file2blockScritti.get(filename);
		if (count == null) { //attenzione con Integer
			count = 0;
		}
		else
			count = count + 1;
		file2blockScritti.put(filename, count);

	}

	public void reset() {
		this.file2blockLetti.clear();
		this.file2blockScritti.clear();
	}

	@Override
	public String toString() {
		return "BlockStats [file2blockLetti=" + file2blockLetti + ", file2blockScritti=" + file2blockScritti + "]";
	}



}

package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

public class NWaysSortPlan extends SortPlan {

	private int vie;
	public NWaysSortPlan(Plan p, List<String> sortfields, Transaction tx) {
		super(p, sortfields, tx);
		this.vie = 4; //mi servono 5 pagine di buffer (da parametrizzare). Usare BufferNeeds
	}

	/**
	 * This method is where most of the action is.
	 * Up to 2 sorted temporary tables are created,
	 * and are passed into SortScan for final merging.
	 * @see simpledb.query.Plan#open()
	 */
	public Scan open() {

		//apro la scan (cioè il nodo) sottostante tramite la plan sottostante.
		Scan src = this.getP().open();

		//divido la mia scan (per esempio TableScan) in vari runs.
		//la splitIntoRuns divide la scan in runs ORDINATI
		int numruns = 8;// da modificare
		List<TempTable> runs = splitIntoRuns(src, numruns); 
		src.close();

		//un passo del merge fonde N=4 runs
		while (runs.size() > this.vie) 
			runs = doAMergeIteration(runs);
		return new NWaysSortScan(runs, this.getComp());
	}

	private List<TempTable> doAMergeIteration(List<TempTable> runs) {
		List<TempTable> result = new ArrayList<TempTable>();
		while (runs.size() > 1) {
			result.add(mergeKRuns(runs, this.vie));
		}
		if (runs.size() == 1)
			result.add(runs.get(0));
		return result;
	}

	private TempTable mergeKRuns(List<TempTable> runs, int k) {
		int runsDaConfrontare;
		TempTable pi = null;
		List<Scan> scanDaconfrontare = new ArrayList<>();
		if (runs.size()>k)
			runsDaConfrontare = k;
		else 
			runsDaConfrontare = runs.size(); //ma dovrebbe essere sempre k. Da testare. In tal caso, togliere la if

		while (runsDaConfrontare > 0) {
			pi = runs.remove(0);
			scanDaconfrontare.add(pi.open());
			runsDaConfrontare--;
		}
		//adesso devo confrontare le varie Scan 
		TempTable result = new TempTable(this.getSch(), this.getTx()); //sarebbe la tabella ordinata (per quelle k run)
		UpdateScan dest = result.open();
        // metto tutti i puntatori al primo record di ogni scan
		firstAllScanRecord(scanDaconfrontare);
		while (!scanDaconfrontare.isEmpty()) {
			removeScanTerminated(scanDaconfrontare);
			//ordino per poter scrivere la scan in cima (avendo più scan, è l'alternativa più veloce)
			Collections.sort(scanDaconfrontare,this.getComp());

			//scrivo
			copy(scanDaconfrontare.get(0), dest);
		}
		dest.close();
		return result;
	}

	private void firstAllScanRecord(List<Scan> scanDaconfrontare) {
		for (Scan src : scanDaconfrontare)
			src.next();	
	}

	private void removeScanTerminated(List<Scan> srci) {
		Iterator<Scan> it = srci.iterator();
		while (it.hasNext()) {
			Scan elemento = it.next();
			boolean hasmore = elemento.next();
			if (!hasmore) {
				elemento.close();
				it.remove();
			}
		}

	}

	//copio il record "con valore minore" nella scan corrente. Avanzo il suo puntatore
	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : this.getSch().fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}



}

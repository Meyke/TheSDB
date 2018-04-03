package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import simpledb.multibuffer.BufferNeeds;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class NWaysSortPlan extends SortPlan {

	private int vie;
	
	public NWaysSortPlan(Plan p, List<String> sortfields, Transaction tx) {
		super(p, sortfields, tx);
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
		this.vie = bufferPerMerge(this.getP());
		System.out.println("merge a "+ this.vie + " vie");

		//divido la mia scan (per esempio TableScan) in vari runs.
		//la splitIntoRuns divide la scan in runs ORDINATI
		int numruns = (this.getP().blocksAccessed()/this.vie) +1;
		List<TempTable> runs = splitIntoRuns(src, numruns);  //non è intelligente. Devo capire il numero di runs. Da calcolare
		System.out.println("numero runs iniziali: " + runs.size());
		src.close();

		//un passo del merge fonde N=4 runs per esempio
		while (runs.size() > this.vie)
			runs = doAMergeIteration(runs);
		return new NWaysSortScan(runs, this.getComp());
	}

	
	private int bufferPerMerge(Plan p) {
		System.out.println("numero blocchi del file = " + p.blocksAccessed());
		int numBuffer = BufferNeeds.bestRoot(p.blocksAccessed());
		return numBuffer;
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
		//adesso devo confrontare le varie Scan (le varie tabelle temporanee)
		TempTable result = new TempTable(this.getSch(), this.getTx()); //sarebbe la tabella ordinata (per quelle k run)
		UpdateScan dest = result.open();
        // metto tutti i puntatori al primo record di ogni scan
		firstAllScanRecord(scanDaconfrontare);
		while (!scanDaconfrontare.isEmpty()) {
			//System.out.println("numero di scans: " + scanDaconfrontare.size());
			//ordino per poter scrivere la scan in cima (avendo più scan, è l'alternativa più veloce)
			Collections.sort(scanDaconfrontare,this.getComp());
			//scrivo
			copy(scanDaconfrontare.get(0), dest);
			removeScanTerminated(scanDaconfrontare);
			//System.out.println("numero di scans dopo il remove: " + scanDaconfrontare.size());
		}
		//System.out.println("esco dal ciclo");
		dest.close();
		return result;
	}

	private void firstAllScanRecord(List<Scan> scanDaconfrontare) {
		for (Scan src : scanDaconfrontare)
			src.beforeFirst();	
		for (Scan src : scanDaconfrontare)
			src.next();	
	}

	private void removeScanTerminated(List<Scan> srci) {
		Iterator<Scan> it = srci.iterator();
		while (it.hasNext()) {
			Scan elemento = it.next();
			boolean hasmore = elemento.isHasMore();//ho dovuto aggiungere questo metodo alle scan, che mi dice solo se la scan ha un ulteriore elemento. E' comodo
			//System.out.println("hasmore = " + hasmore);
			if (!hasmore) {
				elemento.close();
				it.remove();
			}
		}
	}

	//copio il record "con valore minore" nella scan corrente. Avanzo il suo puntatore
	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : this.getSch().fields()) {
			//System.out.println(fldname +" "+ src.getVal(fldname));
			dest.setVal(fldname, src.getVal(fldname));
		}
		return src.next();
	}



}

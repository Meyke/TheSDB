package simpledb.materialize;

import java.util.ArrayList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

public class NWaysSortScan extends SortScan{

	private UpdateScan currentscan=null;
	private RecordComparator comp;
	private List<RID> savedposition;
	List<UpdateScan> scans;
	/**
	 * Creates a sort scan, given a list of k runs now.
	 * If there is only 1 run, then s2 will be null and
	 * hasmore2 will be false.
	 * @param runs the list of runs
	 * @param comp the record comparator
	 */
	public NWaysSortScan(List<TempTable> runs, RecordComparator comp) {
		super();
		System.out.println("ultimo passo del merge ha " + runs.size() + " runs");
		this.comp = comp;
		this.scans = new ArrayList<>(); 
		for (TempTable temptable :runs) {
			UpdateScan s = (UpdateScan) temptable.open();
			s.next();
			this.scans.add(s); 
		}
	}


	/**
	 * Positions the scan before the first record in sorted order.
	 * Internally, it moves to the first record of each underlying scan.
	 * The variable currentscan is set to null, indicating that there is
	 * no current scan.
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		currentscan = null;
		for (UpdateScan s : this.scans)
			s.beforeFirst();
		for (UpdateScan s : this.scans)
			s.next();
	}

	/**
	 * Moves to the next record in sorted order.
	 * First, the current scan is moved to the next record.
	 * Then the lowest record of the two scans is found, and that
	 * scan is chosen to be the new current scan.
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		if (currentscan != null) {
			for (UpdateScan s : this.scans)
				if (currentscan == s)
					s.next();
		}
		removeScanTerminated();
		if (this.scans.isEmpty())
			return false;
		else {
			Collections.sort(this.scans, this.comp);
			currentscan = this.scans.get(0);
		}

		return true;
	}

	private void removeScanTerminated() {
		Iterator<UpdateScan> it = this.scans.iterator();
		while (it.hasNext()) {
			Scan elemento = it.next();
			boolean hasmore = elemento.isHasMore();
			if (!hasmore) {
				it.remove();
				elemento.close();
			}
		}

	}


	/**
	 * Closes the two underlying scans.
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		for (UpdateScan s : this.scans)
			s.close();
	}

	/**
	 * Gets the Constant value of the specified field
	 * of the current scan.
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		return currentscan.getVal(fldname);
	}

	/**
	 * Gets the integer value of the specified field
	 * of the current scan.
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		return currentscan.getInt(fldname);
	}

	/**
	 * Gets the string value of the specified field
	 * of the current scan.
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		return currentscan.getString(fldname);
	}

	/**
	 * Returns true if the specified field is in the current scan.
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return currentscan.hasField(fldname);
	}

	/**
	 * Saves the position of the current record,
	 * so that it can be restored at a later time.
	 */
	public void savePosition() {
		List<RID> rids = new ArrayList<>();
		for (UpdateScan s : this.scans) {
			RID rid = s.getRid();
			rids.add(rid);
		}
		savedposition = rids;
	}

	/**
	 * Moves the scan to its previously-saved position.
	 */
	public void restorePosition() {
		int i = 0;
		for (UpdateScan s : this.scans) {
			RID rid1 = savedposition.get(i);
			s.moveToRid(rid1);
			i++;
		}
	}


}

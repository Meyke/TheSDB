package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import simpledb.parse.QueryData;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeSortTest {
	public static void main(String[] args) {


		SimpleDB.init("studentdb");

		// analogous to the connection
		Transaction tx = new Transaction();

		Collection<String> fields = new ArrayList<>();
		fields.add("bid");
		fields.add("bname");
		
		Collection<String> tables = new ArrayList<>();
		tables.add("bigtable");


		QueryData data = new QueryData(fields, tables, new Predicate());

		//Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (Iterator<String> iterator = data.tables().iterator(); iterator.hasNext(); ) {
			String tblname = iterator.next();
			plans.add(new TablePlan(tblname, tx));
		}

		//Step 2: Create the product of all table plans

		Plan p = null;
		List<String> sortfields = new ArrayList<>();
		sortfields.add("bname");
		
		
		for (Plan nextplan : plans)
			p = new NWaysSortPlan(nextplan, sortfields, tx);
			//p = new SortPlan(nextplan, sortfields, tx);

		Scan s = p.open(); //ritorna una NWaysSortScan

		System.out.println("\nla scan è una: " + s.getClass() + "\n");

		for (String str : fields) {
			System.out.print(str +"\t\t");
		}
		System.out.println();
		for (int i=0; i<fields.size(); i++)
			System.out.print("-------------");
		System.out.println();
		//faccio la next della NWaysSortScan
		while(s.next()) {
			for (String st : fields) {
				System.out.print(s.getVal(st) + "\t\t");
			}
			System.out.println();
		}

	}
}

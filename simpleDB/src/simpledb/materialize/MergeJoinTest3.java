package simpledb.materialize;

import simpledb.parse.QueryData;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MergeJoinTest3 {

	public static void main(String[] args) {


		SimpleDB.init("studentdb");

		// analogous to the connection
		Transaction tx = new Transaction();

		Collection<String> fields = new ArrayList<>();
		fields.add("sname");
		//fields.add("dname");
		fields.add("majorid");
		//fields.add("did");
		Collection<String> tables = new ArrayList<>();

		tables.add("student");
		//tables.add("dept");

		QueryData data = new QueryData(fields, tables, new Predicate());
		//new QueryData(fields, tables, new Predicate(), new ArrayList<>());

		//Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (Iterator<String> iterator = data.tables().iterator(); iterator.hasNext(); ) {
			String tblname = iterator.next();
			plans.add(new TablePlan(tblname, tx));
		}

		//Step 2: Create the product of all table plans
		//Plan p = plans.remove(0);
		Plan p = null;
		List<String> sortfields = new ArrayList<>();
		sortfields.add("sname");
		for (Plan nextplan : plans)
			p = new NWaysSortPlan(nextplan, sortfields, tx);
			//p = new SortPlan(nextplan, sortfields, tx);

		//Step 3: Add a selection plan for the predicate
		//p = new SelectPlan(p, data.pred());

		//Step 4: Project on the fields names
		//p = new ProjectPlan(p, data.fields());

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
package studentClient.simpledb;

import java.util.Random;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateBigTableNoServer {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.init("studentdb");

			// analogous to the connection
			Transaction tx = new Transaction();

			String s1 = "create table BIGTABLE(BId int, BNAME varchar(10))";
			SimpleDB.planner().executeUpdate(s1, tx);
			System.out.println("Table BIGTABLE created.");


			for (int i=0; i<10000; i++) {
				Integer id = (i+1);
				String bid = id.toString();
				String nome = generatoreCasualeDisStringhe();
				//String v = "("+ bid + ", \'"+randomStringGenerator()+")";
				String s = "insert into BIGTABLE(BId, BNAME) values ( "+bid+ ", '"+nome+"')";
				System.out.println(s);
				SimpleDB.planner().executeUpdate(s, tx);
			}

			System.out.println("inserted");

			String qry = "select BId, BNAME "
					+ "from BIGTABLE";	
			Plan p = SimpleDB.planner().createQueryPlan(qry, tx);

			// analogous to the result set
			Scan sc = p.open();
			
			System.out.println("id\tName");
			while (sc.next()) {
				String sid = sc.getVal("bid").toString();
				String sname = sc.getString("bname"); //SimpleDB stores field names	
				System.out.println(sid + "\t" + sname);
			}
			sc.close();

			tx.commit();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static String generatoreCasualeDisStringhe() {
		String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder sb = new StringBuilder();
		Random rnd = new Random();
		int varlength = rnd.nextInt(10) + 1;
		while (sb.length() < varlength) {
			int index = (int) (rnd.nextFloat() * alphabet.length());
			sb.append(alphabet.charAt(index));
		}
		String randString = sb.toString();
		return randString;
	}
}
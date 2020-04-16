package DBuggers;

import java.awt.Polygon;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

import bTree.Ref;

public class DBAppTest {
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		DBApp db  =new DBApp();		
		Hashtable<String,String> htbl = new Hashtable<>();
		htbl.put("ID", "java.lang.Integer");
		htbl.put("name", "java.lang.String");
		htbl.put("age","java.lang.Integer");
		htbl.put("gpa", "java.lang.Double");
		htbl.put("location", "java.awt.Polygon");
		db.createTable("T", "ID", htbl);
		db.createBTreeIndex("T", "age");
		db.createRTreeIndex("T", "location");

		Hashtable <String,Object> row = new Hashtable<>();
		row.put("ID", 37);
		row.put("name", "amumumu");
		row.put("age", 400);
		row.put("gpa", 4.2);
		int x1[]= {1,2,3,4};
		int y1[]= {3,1,4,6};
		row.put("location", new Polygon(x1,y1,4));
		db.insertIntoTable("T", row);
		
		
		row = new Hashtable<String, Object>();
		row.put("ID", 24);
		row.put("name", "lux");
		row.put("age", 14);
		row.put("gpa", 1.29);
		int x2[]= {1,4,2,6,8};
		int y2[]= {2,9,5,1,2};
		row.put("location", new Polygon(x2,y2,5));
		db.insertIntoTable("T", row);
		
		row = new Hashtable<String, Object>();
		row.put("ID", 5);
		row.put("name", "shaimaa");
		row.put("age", 12);
		row.put("gpa", 6.4);
		int x3[]= {1,6,8};
		int y3[]= {2,1,2};
		row.put("location", new Polygon(x3,y3,3));

		db.insertIntoTable("T", row);
		
		row = new Hashtable<String, Object>();
		row.put("ID", 100);
		row.put("name", "shaimaa");
		row.put("age", 69);
		row.put("gpa", 0.96);
		int x4[]= {10,1,4,2,6,8};
		int y4[]= {100,2,9,5,1,2};
		row.put("location", new Polygon(x4,y4,6));

		db.insertIntoTable("T", row);
		
		row = new Hashtable<String, Object>();
		row.put("ID", 45);
		row.put("name", "Matilda");
		row.put("age", 69);
		row.put("gpa", 0.96);
		int x5[]= {2,6,8};
		int y5[]= {5,1,2};
		row.put("location", new Polygon(x5,y5,3));

		db.insertIntoTable("T", row); 
		
		row = new Hashtable<String, Object>();
		row.put("ID", 37);
		row.put("name", "diana");
		row.put("age", 13);
		row.put("gpa",0.62);
		int x6[]= {6,8};
		int y6[]= {1,2};
		row.put("location", new Polygon(x6,y6,2));

		db.insertIntoTable("T", row);
		
		row = new Hashtable<String, Object>();
		row.put("ID", 37);
		row.put("name", "diana");
		row.put("age", 41);
		row.put("gpa", 0.62);
		int x7[]= {8};
		int y7[]= {2};
		row.put("location", new Polygon(x7,y7,1));

		db.insertIntoTable("T", row);
		
		row = new Hashtable<String, Object>();
		row.put("ID", 37);
		row.put("name", "diana");
		row.put("age", 27);
		row.put("gpa", 7.3);
		int x8[]= {13,8,190};
		int y8[]= {17,2,263};
		row.put("location", new Polygon(x8,y8,3));

		db.insertIntoTable("T", row);
		
//		row = new Hashtable<String, Object>();
//		row.put("ID", 37);
//		row.put("name", "diana");
//		row.put("age", 30);
//		row.put("gpa", 0.62);
//		db.insertIntoTable("T", row);
//		
//		
//		row = new Hashtable<String, Object>();
//		row.put("ID", 37);
//		row.put("name", "diana");
//		row.put("age", 58);
//		row.put("gpa", 0.62);
//		db.insertIntoTable("T", row);
//		
//		row = new Hashtable<String, Object>();
//		row.put("ID", 37);
//		row.put("name", "chloe");
//		row.put("age",16);
//		row.put("gpa", 3.6);
//		db.insertIntoTable("T", row);
//		
//		row = new Hashtable<String, Object>();
//		row.put("ID", 40);
//		row.put("name", "zizo");
//		row.put("age", 21);
//		row.put("gpa", 1.59);
//		db.insertIntoTable("T", row);
	
	Table	table = (Table)(DBApp.deSerialize("T"));
		table.printTable();
		Hashtable <String,Object>  htb = new Hashtable<>();
		htb.put("name", "diana");
		
		db.deleteFromTable("T", htb);
		table = (Table) DBApp.deSerialize("T");
		table.printTable();
//		SQLTerm s[]= new SQLTerm[1];
//		 s[0] = new SQLTerm("T", "age", "=", 69);
//		Iterator<Tuple> i = db.selectFromTable(s, null); 
//		while(i.hasNext()) {
//			System.out.println(i.next());
//		}
//		table = (Table) DBApp.deSerialize("T");
//		table.printTable();
//		
	}
}
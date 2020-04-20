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
		DBApp db = new DBApp();
		Hashtable<String, String> htbl = new Hashtable<>();
		// htbl.put("ID", "java.lang.Integer");
		// htbl.put("name", "java.lang.String");
		// htbl.put("age", "java.lang.Integer");
		// htbl.put("gpa", "java.lang.Double");
		// db.createTable("T", "ID", htbl);
		// db.createBTreeIndex("T", "ID");
		//
		 Hashtable<String, Object> row = new Hashtable<>();
		// row.put("ID", 37);
		// row.put("name", "amumumu");
		// row.put("age", 400);
		// row.put("gpa", 1.11);
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 50);
		// row.put("name", "lux");
		// row.put("age", 14);
		// row.put("gpa", 1.29);
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 5);
		// row.put("name", "shaimaa");
		// row.put("age", 12);
		// row.put("gpa", 6.4);
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 1);
		// row.put("name", "shaimaa");
		// row.put("age", 34);
		// row.put("gpa", 0.96);
		//
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 45);
		// row.put("name", "Matilda");
		// row.put("age", 69);
		// row.put("gpa", 0.96);
		//
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 37);
		// row.put("name", "diana");
		// row.put("age", 13);
		// row.put("gpa", 0.62);
		//
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 37);
		// row.put("name", "diana");
		// row.put("age", 41);
		// row.put("gpa", 0.62);
		//
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 37);
		// row.put("name", "diana");
		// row.put("age", 27);
		// row.put("gpa", 7.3);
		//
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 37);
		// row.put("name", "diana");
		// row.put("age", 30);
		// row.put("gpa", 0.62);
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 0);
		// row.put("name", "ko");
		// row.put("age", 75);
		// row.put("gpa", 1.1);
		// db.insertIntoTable("T", row);
		//
		// row = new Hashtable<String, Object>();
		// row.put("ID", 137);
		// row.put("name", "chloe");
		// row.put("age", 162);
		// row.put("gpa", 3.26);
		// db.insertIntoTable("T", row);
		//
		 row = new Hashtable<String, Object>();
		 row.put("ID", 1);
		 row.put("name", "bora");
		 row.put("age", 21);
		 row.put("gpa", 1.9);
		 db.insertIntoTable("T", row);
		// Table table = (Table) (DBApp.deSerialize("T"));
		// table.printTable();
		//
		// Hashtable<String, Object> d = new Hashtable<>();
		// d.put("ID", 37);
		// d.put("age", 24);
		// db.deleteFromTable("T", d);
		Table table = (Table) (DBApp.deSerialize("T"));
		 table.printTable();
		 Hashtable <String,Object> u = new Hashtable<>();
		 u.put("age", 69);
		 u.put("name", "lolo");
		 
		 db.updateTable("T", "1000000", u);
		 
		 table = (Table) (DBApp.deSerialize("T"));
		 table.printTable();
		 
		// db.createRTreeIndex("Makan", "Location");
		// htbl.put("Location", "java.awt.Polygon");
		// htbl.put("ID", "java.lang.Integer");
		// db.createTable("Makan", "Location", htbl);
		//
//		int[] x = { 0, 1, 1, 0 };
//		int[] y = { 0, 0, 1, 1 };
//		Polygon poly = new Polygon(x, y, 4);
//
//		Hashtable<String, Object> row = new Hashtable<String, Object>();
//		row.put("ID", 6);
//		row.put("Location", poly);
//		db.insertIntoTable("Makan", row);

		// int[] x2 = {0, 2, 2, 0};
		// int[] y2 = {0, 0, 2, 2};
		// Polygon poly = new Polygon(x2, y2, 4);
		// row.put("ID", 302);
		// row.put("Location", poly);
		// db.insertIntoTable("Makan", row);
		//
		// int[] x3 = {0, 1};
		// int[] y3 = {0, 0};
		// Polygon poly = new Polygon(x3, y3, 2);
		// row.put("ID", 500);
		// row.put("Location", poly);
		// db.insertIntoTable("Makan", row);
		//
		// int[] x4 = {-1, 0, 0, -1};
		// int[] y4 = {-1, -1, 0, 0};
		// poly = new Polygon(x4, y4, 4);
		// row.put("ID", 0);
		// row.put("Location", poly);
		// db.insertIntoTable("Makan", row);
		//
		// int[] x5 = {0, 1};
		// int[] y5 = {0, 0};
		// poly = new Polygon(x5, y5, 2);
		// row.put("ID", 33);
		// row.put("Location", poly);
		// db.insertIntoTable("Makan", row);
		//
		// int[] x6 = {0, 2, 2, 0};
		// int[] y6 = {0, 0, 1, 1};
		// poly = new Polygon(x6, y6, 4);
		// row.put("ID", 5);
		// row.put("Location", poly);
		// db.insertIntoTable("Makan", row);
		//
		//
//		Table table = (Table)(DBApp.deSerialize("Makan"));
//		table.printTable();
//		htbl.put("Date", "java.util.Date");
//		htbl.put("ID", "java.lang.Integer");
//		db.createTable("waat", "Date", htbl);
//		db.createBTreeIndex("waat", "Date");
//		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
//		Date date = formatter.parse("12/03/2021");
//
//		row.put("Date", date);
//		row.put("ID", 134);
//		db.insertIntoTable("waat", row);
//
//		Table table = (Table) (DBApp.deSerialize("waat"));
//		table.printTable();

	}
}
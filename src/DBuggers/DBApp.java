package DBuggers;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;

import RTree.RTree;
import bTree.BPTree;
import bTree.Ref;

public class DBApp {

	static int MaximumRowsCountinPage;
	static int NodeSize;

	public DBApp() throws IOException {
		init();
	}

	public void init() throws IOException {

		FileReader reader = new FileReader("config/DBApp.properties");

		Properties p = new Properties();
		p.load(reader);

		MaximumRowsCountinPage = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
		NodeSize = Integer.parseInt(p.getProperty("NodeSize"));
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
		Table table = new Table(strTableName, strClusteringKeyColumn);
		serializeTable(table);

		FileWriter writer = new FileWriter("data/metadata.csv", true);

		for (Map.Entry<String, String> e : htblColNameType.entrySet()) {

			writer.append(strTableName + ',');
			writer.append(e.getKey() + ',');
			writer.append((e.getValue().equals("java.awt.Polygon") ? "DBuggers.PolygonModified" : e.getValue()) + ',');
			writer.append(e.getKey().equals(strClusteringKeyColumn) ? "True," : "False,");
			writer.append("False\n");

		}
		writer.close();
	}

	public void createRTreeIndex(String strTableName, String strColName)
			throws DBAppException, ClassNotFoundException, IOException {
		Table table = (Table) deSerialize(strTableName);
		String Type = getType(strTableName, strColName);
		if (!Type.equals("DBuggers.PolygonModified")) {
			throw new DBAppException("error: R tree must be created on spatial data types ");
		}
		RTree<DBuggers.PolygonModified> r = new RTree<PolygonModified>(strColName, NodeSize);
		table.RTreeIndices.add(r);
		fillIndex(strColName, r, table);
		serializeTable(table);
	}

	public void createBTreeIndex(String strTableName, String strColName)
			throws DBAppException, IOException, ClassNotFoundException {
		String Type = getType(strTableName, strColName);
		if (Type.equals("DBuggers.PolygonModified")) {
			throw new DBAppException("error: B+ tree must be created on non spatial data types ");
		}
		Table table = (Table) deSerialize(strTableName);
		BPTree btree = null; // not sure whether to include the type when declaring the b tree
		switch (Type) {
		case "java.lang.Integer":
			table.getBTreeIndices().add(btree = new BPTree<Integer>(strColName, NodeSize));
			break;
		case "java.lang.Double":
			table.getBTreeIndices().add(btree = new BPTree<Double>(strColName, NodeSize));

			break;
		case "java.lang.Boolean":
			table.getBTreeIndices().add(btree = new BPTree<Boolean>(strColName, NodeSize));

			break;
		case "java.util.Date":
			table.getBTreeIndices().add(btree = new BPTree<Date>(strColName, NodeSize));

			break;
		default:
			table.getBTreeIndices().add(btree = new BPTree<String>(strColName, NodeSize));
			break;
		}
		fillIndex(strColName, btree, table);
		serializeTable(table);

	}

	private void fillIndex(String strColName, Object tree, Table table) throws ClassNotFoundException, IOException {

		for (String pageAddress : table.pageAddresses) {
			Page page = (Page) deSerialize(pageAddress);
			for (int i = 0; i < page.getSize(); i++) {
				Ref ref = new Ref(pageAddress, i);
				Tuple tuple = page.getRecords().get(i);
				if (tree instanceof BPTree) {
					BPTree btree = (BPTree) tree;
					btree.insert((Comparable) tuple.getColNameValue().get(strColName), ref);
				} else {
					RTree rTree = (RTree) tree;
					rTree.insert((Comparable) tuple.getColNameValue().get(strColName), ref);
				}
			}
		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		Table table = (Table) deSerialize(strTableName);
		Vector<String> pageAddresses = table.pageAddresses;
		Tuple tuple = new Tuple(htblColNameValue, strTableName);
		Object[] re = insertFirst(tuple, table);
		int idx = (int) re[0];
		Tuple newRecord = (Tuple) re[1];
		Page prevPage = idx == 0 ? null : (Page) deSerialize(pageAddresses.get(idx - 1));
		boolean first = tuple == newRecord;
		boolean newTupleInserted = newRecord == null; // A flag to check if the tuple has placed successfully

		for (int i = idx; i < pageAddresses.size(); i++) {
			String pageAddress = pageAddresses.get(i);
			Page page = (Page) deSerialize(pageAddress);

			// to check the case when I am smaller than the smallest element in this page
			// and the previous page has empty space
			// so we add it in the prevPage
			if (prevPage != null && newRecord.compareTo(page.getFirst()) < 0
					&& prevPage.getSize() < MaximumRowsCountinPage) {
				prevPage.insert(newRecord);
				InsertInAllIndicies(prevPage.getSize() - 1, prevPage, newRecord, table);
				newTupleInserted = true;
				serializePage(prevPage);
				break;
			}

			// if the tuple I am inserting is bigger than all tuples so I can add it on the
			// last page
			if (pageAddress.equals(pageAddresses.lastElement()) && page.getSize() < MaximumRowsCountinPage
					&& newRecord.compareTo(page.getLast()) > 0) {
				page.insert(newRecord);
				InsertInAllIndicies(page.getSize() - 1, page, newRecord, table);
				newTupleInserted = true;
				serializePage(page);
				break;
			}

			// if the tuple I am inserting is bigger than all tuples in this page so its
			// place is in a coming page
			if (newRecord.compareTo(page.getLast()) > 0) {
				prevPage = page;
				continue;
			}

			Object[] res = page.insert(newRecord);
			int index = (int) res[0];
			if (first) {
				InsertInAllIndicies(index, page, newRecord, table);
				first = false;
			} else {
				updateInAllIndicies(prevPage, pageAddress, newRecord, table);
			}
			updateRecords(index, page, pageAddress, table);
			newRecord = (Tuple) res[1];
			serializePage(page);
			prevPage = page;

			// tuple inserted successfully and there is no shift
			if (newRecord == null) {
				newTupleInserted = true;
				break;
			}
		}
		// the last page is full and newRecord is bigger than all records in table
		if (!newTupleInserted) {
			Page page = new Page(strTableName, table.count++);
			table.pageAddresses.add(page.tableName + "_page" + page.pageNumber);
			page.insert(newRecord);
			if (first) {
				InsertInAllIndicies(0, page, newRecord, table);
			} else
				updateInAllIndicies(prevPage, page.tableName + "_page" + page.pageNumber, newRecord, table);
			serializePage(page);
		}
		serializeTable(table);
	}

	private Object hasIndex(Table table) {
		Object clusteringKeyTree = null;
		for (BPTree tree : table.BTreeIndices) {
			if (tree.colName.equals(table.clusteringKeyName)) {
				clusteringKeyTree = tree;
			}
		}
		for (RTree tree : table.RTreeIndices) {
			if (tree.colName.equals(table.clusteringKeyName)) {
				clusteringKeyTree = tree;
			}
		}
		return clusteringKeyTree;
	}

	private Object[] insertFirst(Tuple tuple, Table table) throws ClassNotFoundException, IOException {
		Object clusteringKeyTree = hasIndex(table);
		if (clusteringKeyTree == null) {
			Object[] re = { 0, tuple };
			return re;
		}
		Ref ref = null;
		if (clusteringKeyTree instanceof BPTree)
			ref = ((BPTree) clusteringKeyTree).ceiling((Comparable) tuple.clusteringKey);
		else
			ref = ((RTree) clusteringKeyTree).ceiling((Comparable) tuple.clusteringKey);

		Vector<String> pageAddresses = table.pageAddresses;

		if (pageAddresses.size() == 0) {
			Object[] re = { 0, tuple };
			return re;
		}
		if (ref == null) {
			Object[] re = { pageAddresses.size() - 1, tuple };
			return re;
		}
		int low = 0;
		int hi = pageAddresses.size() - 1;
		int mid = 0;
		int refno = Integer.parseInt(ref.getPage().substring(table.tableName.length() + 5));
		while (low <= hi) {
			mid = low + hi >> 1;
			if (pageAddresses.get(mid).equals(ref.getPage()))
				break;
			int midno = Integer.parseInt(pageAddresses.get(mid).substring(table.tableName.length() + 5));
			if (refno > midno)
				low = midno + 1;
			else
				hi = midno - 1;
		}

		Page page = (Page) deSerialize(ref.getPage());
		if (mid == 0) {
			Object[] res = page.insert(tuple);
			int index = (int) res[0];
			InsertInAllIndicies(index, page, tuple, table);
			updateRecords(index, page, ref.getPage(), table);
			Tuple newRecord = (Tuple) res[1];
			serializePage(page);
			Object[] re = { 1, newRecord };
			return re;
		}
		Page prevPage = (Page) deSerialize(pageAddresses.get(mid - 1));
		if (tuple.compareTo(page.getFirst()) < 0 && prevPage.getSize() < MaximumRowsCountinPage) {
			prevPage.insert(tuple);
			InsertInAllIndicies(prevPage.getSize() - 1, prevPage, tuple, table);
			serializePage(prevPage);
			Object[] re = { pageAddresses.size(), null };
			return re;
		}
		Object[] res = page.insert(tuple);
		int index = (int) res[0];
		InsertInAllIndicies(index, page, tuple, table);
		updateRecords(index, page, ref.getPage(), table);
		Tuple newRecord = (Tuple) res[1];
		serializePage(page);
		Object[] re = { mid + 1, newRecord };
		return re;
	}

	private void updateInAllIndicies(Page oldPage, String newPageAddress, Tuple tuple, Table table) {
		String pageAddress = oldPage.tableName + "_page" + oldPage.pageNumber;
		Ref oldRef = new Ref(pageAddress, MaximumRowsCountinPage - 1);
		Ref newRef = new Ref(newPageAddress, 0);

		for (BPTree bt : table.BTreeIndices)
			bt.updateRef((Comparable) tuple.getColNameValue().get(bt.colName), oldRef, newRef);

		for (RTree tree : table.RTreeIndices)
			tree.updateRef((Comparable) tuple.getColNameValue().get(tree.colName), oldRef, newRef);
	}

	private void InsertInAllIndicies(int index, Page page, Tuple tuple, Table table) {
		String pageAddress = page.tableName + "_page" + page.pageNumber;
		Ref ref = new Ref(pageAddress, index);

		for (BPTree bt : table.BTreeIndices)
			bt.insert((Comparable) tuple.getColNameValue().get(bt.colName), ref);

		for (RTree tree : table.RTreeIndices)
			tree.insert((Comparable) tuple.getColNameValue().get(tree.colName), ref);

	}

	private void updateRecords(int index, Page page, String pageAddress, Table table) {
		Vector<Tuple> records = page.records;
		for (int i = index + 1; i < records.size(); i++) {

			Ref oldRef = new Ref(pageAddress, i - 1);
			Ref newRef = new Ref(pageAddress, i);

			for (BPTree bt : table.BTreeIndices)
				bt.updateRef((Comparable) records.get(i).getColNameValue().get(bt.colName), oldRef, newRef);

			for (RTree tree : table.RTreeIndices)
				tree.updateRef((Comparable) records.get(i).getColNameValue().get(tree.colName), oldRef, newRef);
		}
	}

	static void serializeTable(Table table) throws IOException {
		String newTableName = "classes/DBuggers/" + table.tableName + ".class";

		File yourFile = new File(newTableName);
		yourFile.createNewFile();

		FileOutputStream fileOut = new FileOutputStream(newTableName);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(table);
		out.close();
		fileOut.close();
	}

	static void serializePage(Page page) throws IOException {
		String newPageName = "classes/DBuggers/" + page.tableName + "_page" + page.pageNumber + ".class";

		File yourFile = new File(newPageName);
		yourFile.createNewFile();

		FileOutputStream fileOut = new FileOutputStream(newPageName);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(page);
		out.close();
		fileOut.close();
	}

	static Object deSerialize(String path) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream("classes/DBuggers/" + path + ".class");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Object o = in.readObject();
		in.close();
		fileIn.close();
		return o;
	}

	private static Object parser(String s, String strTableName) throws ParseException, IOException {

		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		StringBuilder csvContent = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			csvContent.append(line).append("\n");
		br.close();
		TreeSet<String> set = new TreeSet<>();

		String type = "";

		for (String colInfo : csvContent.toString().split("\n")) {
			String[] array = colInfo.split(", *");

			if (array[0].equals(strTableName) && array[3].equals("True")) {
				type = array[2];
				break;
			}
		}

		switch (type) {
		case "java.lang.Integer":
			return Integer.parseInt(s);
		case "java.lang.Double":
			return Double.parseDouble(s);
		case "java.lang.Boolean":
			return Boolean.parseBoolean(s);
		case "java.util.Date":
			Date date = new SimpleDateFormat("yyyy-MM-dd").parse(s);
			return date;
		case "DBuggers.PolygonModified":
			StringTokenizer st = new StringTokenizer(s, ",)(");
			int n = st.countTokens();
			int[] xs = new int[n / 2];
			int[] ys = new int[n / 2];
			for (int i = 0; i < n / 2; i++) {
				xs[i] = Integer.parseInt(st.nextToken());
				ys[i] = Integer.parseInt(st.nextToken());
			}

			return new PolygonModified(new Polygon(xs, ys, n / 2));
		default:
			return s;
		}

	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException, ParseException {
		Table table = (Table) deSerialize(strTableName);
		inputValidity(strTableName, htblColNameValue);
		Comparable updateRecord = (Comparable) parser(strClusteringKey, strTableName);

		Vector<String> pageAddresses = table.pageAddresses;

		Object clusteringKeyTree = hasIndex(table);

		if (clusteringKeyTree == null) {
			for (String pageAddress : pageAddresses) {
				Page page = (Page) deSerialize(pageAddress);
				if (((Comparable) page.getLast().clusteringKey).compareTo(updateRecord) < 0)
					continue;
				Vector<Tuple> records = page.records;
				int low = 0;
				int hi = records.size() - 1;
				int pos = -1;
				while (low <= hi) {
					int mid = low + hi >> 1;

					Comparable midRecord = (Comparable) records.get(mid).clusteringKey;

					if (midRecord.equals(updateRecord)) {
						hi = mid - 1;
						pos = mid;
					}
					else if (midRecord.compareTo(updateRecord) > 0)
						hi = mid - 1;
					else
						low = mid + 1;
				}
				if (pos == -1)
					break;
				int i = pos;
				boolean last = true;
				for (Tuple tuple : records.subList(pos, records.size())) {
					Comparable posRecord = (Comparable) tuple.clusteringKey;
					if (!posRecord.equals(updateRecord)) {
						last = false;
						break;
					}
					Ref ref = new Ref(pageAddress, i++);
					updateRecord(table, htblColNameValue, tuple, ref);
				}
				serializePage(page);
				if (!last)
					break;
			}
		} else {
			Vector<Ref> refs = null;
			if (clusteringKeyTree instanceof BPTree)
				refs = ((BPTree) clusteringKeyTree).search(updateRecord);
			else
				refs = ((RTree) clusteringKeyTree).search(updateRecord);
			Collections.sort(refs);
			int index = 0;
			for (String pageAddress : pageAddresses) {
				Page page = null;
				while (index < refs.size() && pageAddress.equals(refs.get(index).getPage())) {

					if (page == null)
						page = (Page) deSerialize(pageAddress);
					Ref ref = refs.get(index);
					int tuplePos = ref.getIndexInPage();
					updateRecord(table, htblColNameValue, page.records.get(tuplePos), ref);

					index++;
				}
				if (page != null)
					serializePage(page);
			}
		}
		serializeTable(table);
	}

	private static void updateRecord(Table table, Hashtable<String, Object> htblColNameValue, Tuple tuple, Ref ref) {
		tuple.TouchDate = new Date();

		for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
			String colName = e.getKey();
			Comparable oldValue = (Comparable) tuple.colNameValue.get(colName);
			Comparable newValue = null;
			if (e.getValue() instanceof java.awt.Polygon) {
				newValue = (Comparable) new PolygonModified((Polygon) e.getValue());
			} else
				newValue = (Comparable) e.getValue();
			tuple.colNameValue.put(e.getKey(), newValue);
			for (BPTree tree : table.BTreeIndices)
				if (tree.colName.equals(colName)) {
					tree.delete(oldValue, ref);
					tree.insert(newValue, ref);
				}
			for (RTree tree : table.RTreeIndices)
				if (tree.colName.equals(colName)) {
					tree.delete(oldValue, ref);
					tree.insert(newValue, ref);
				}
		}
	}

	private static void inputValidity(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, DBAppException {
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		StringBuilder csvContent = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			csvContent.append(line).append("\n");
		br.close();
		TreeSet<String> set = new TreeSet<>();

		for (String colInfo : csvContent.toString().split("\n")) {
			String[] array = colInfo.split(", *");

			if (!array[0].equals(strTableName))
				continue;

			set.add(array[1]);

			if (htblColNameValue.containsKey(array[1])) {
				Object colVal = htblColNameValue.get(array[1]);
				Class cls = colVal.getClass();
				String type = cls.getName();
				if (!type.equals(array[2])) {
					if (!type.equals("java.awt.Polygon")) {
						if (!array[2].equals("DBuggers.PolygonModified"))
							throw new DBAppException("Incompatible type: Expected \"" + array[2] + "\" found \"" + type + "\"");
						else
							throw new DBAppException(
									"Incompatible type: Expected \"" + "java.awt.Polygon" + "\" found \"" + type + "\"");
					} else if ((type.equals("java.awt.Polygon") && !array[2].equals("DBuggers.PolygonModified"))) {
						throw new DBAppException(
								"Incompatible type: Expected \"" + array[2] + "\" found \"" + type + "\"");
					}
				}

			}
		}
		for (String k : htblColNameValue.keySet())
			if (!set.contains(k))
				throw new DBAppException("ERROR: Column \"" + k + "\n not found.");
	}

	private void deleteFromTableUsingNoIndex(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {
		Table table = (Table) deSerialize(strTableName);
		Vector<String> pageAddresses = table.pageAddresses;
		// loop over all page addresses
		ArrayList<String> remPage = new ArrayList<>();
		for (String pageAddress : pageAddresses) {
			Page page = (Page) deSerialize(pageAddress);
			// loop over all tuples in the page's vector of tuples
			all: for (int i = 0; i < page.getSize(); i++) {
				Tuple row = page.getRecords().get(i);
				for (Map.Entry e : htblColNameValue.entrySet()) {
					Comparable thisTuple = (Comparable) e.getValue(); // the delete condition
					Comparable searchTuple = (Comparable) row.colNameValue.get(e.getKey()); // to be found in table
					if (thisTuple.compareTo(searchTuple) != 0) {
						continue all;
					}
				}

				Ref ref = new Ref(pageAddress, i);
				for (BPTree tree : table.getBTreeIndices()) {
					tree.delete((Comparable) row.colNameValue.get(tree.colName), ref);

				}
				for (int j = i + 1; j < page.getSize(); j++) {
					for (BPTree tree : table.getBTreeIndices()) {
						tree.updateRef((Comparable) page.getRecords().get(j).colNameValue.get(tree.colName),
								new Ref(pageAddress, j), new Ref(pageAddress, j - 1));
					}
				}
				for (RTree tree : table.getRTreeIndices()) {
					tree.delete((Comparable) row.colNameValue.get(tree.colName), ref);

				}
				for (int j = i + 1; j < page.getSize(); j++) {
					for (RTree tree : table.getRTreeIndices()) {
						tree.updateRef((Comparable) page.getRecords().get(j).colNameValue.get(tree.colName),
								new Ref(pageAddress, j), new Ref(pageAddress, j - 1));
					}
				}
				page.getRecords().remove(i);
				i--;
				if (page.records.isEmpty()) {
					remPage.add(pageAddress);
					String newPageName = "classes/DBuggers/" + page.tableName + "_page" + page.pageNumber + ".class";
					File f = new File(newPageName);
					f.delete();
					serializeTable(table);
				} else
					serializePage(page);
			}

		}

		pageAddresses.removeAll(remPage);
		serializeTable(table);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void deleteFromTableClusteringKey(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, IOException {
		Table table = (Table) deSerialize(strTableName);
		Vector<String> pageAddresses = table.pageAddresses;
		// loop over all page addresses
		Page page = null;
		Vector<String> remPage = new Vector<>();
		int idx = -1;
		int pagenum = -1;
		for (String pageAddress : pageAddresses) {
			++pagenum;
			page = (Page) deSerialize(pageAddress);
			// loop over all tuples in the page's vector of tuples
			int lo = 0;
			int hi = page.getSize() - 1;
			while (lo <= hi) {
				int mid = lo + hi >> 1;
				if (((Comparable) (page.getRecords().get(mid).clusteringKey))
						.compareTo((Comparable) htblColNameValue.get(table.clusteringKeyName)) >= 0) {
					idx = mid;
					hi = mid - 1;
				} else {
					lo = mid + 1;
				}
			}
			if (idx != -1)
				break;
		}
		/*
		 * The clustering key that i am trying to delete upon does not exist so return
		 */
		if (idx == -1
				|| !page.getRecords().get(idx).clusteringKey.equals(htblColNameValue.get(table.getClusteringKey())))
			return;

		ArrayList<pair> potentialTBD = new ArrayList<>();
		boolean broke = false;
		for (int i = idx; i < page.getSize(); i++) {
			if (page.getRecords().get(idx).clusteringKey.equals(htblColNameValue.get(table.getClusteringKey()))) {
				pair info = new pair(page.getRecords().get(i), new Ref(pageAddresses.get(pagenum), i));
				potentialTBD.add(info);
			} else {
				broke = true;
				break;
			}
		}
		if (!broke)
			all: for (int i = pagenum + 1; i < table.pageAddresses.size(); i++) {
				page = (Page) deSerialize(table.pageAddresses.get(i));
				for (int j = 0; j < page.getSize(); j++) {
					if (page.getRecords().get(j).clusteringKey.equals(htblColNameValue.get(table.getClusteringKey()))) {
						pair info = new pair(page.getRecords().get(j), new Ref(pageAddresses.get(i), j));
						potentialTBD.add(info);
					} else {
						break all;
					}
				}
			}

		Vector<pair> TBDFromPotential = new Vector<>();
		all: for (pair info : potentialTBD) {
			Tuple row = info.tuple;
			Ref ref = info.ref;
			for (Map.Entry e : htblColNameValue.entrySet()) {
				Comparable thisTuple = (Comparable) e.getValue(); // the delete condition
				Comparable searchTuple = (Comparable) row.colNameValue.get(e.getKey()); // to be found in the
																						// potentially to be deleted
																						// rows
				if (thisTuple.compareTo(searchTuple) != 0) {
					TBDFromPotential.add(info);
					continue all;
				}
			}
		}
		potentialTBD.removeAll(TBDFromPotential);
		Collections.sort(potentialTBD);
		for (int i = 0; i < potentialTBD.size(); i++) {
			pair p = potentialTBD.get(i);
			Ref ref = p.ref;
			page = (Page) deSerialize(ref.getPage());
			for (BPTree btree : table.getBTreeIndices()) { // deleting the ref of the instance to be removed from the
															// table
				btree.delete((Comparable) p.tuple.colNameValue.get(btree.colName), p.ref);
			}
			for (int j = ref.getIndexInPage() + 1; j < page.getSize(); j++) { // shifting all records in the tree
				for (BPTree btree : table.getBTreeIndices()) {
					btree.updateRef((Comparable) page.getRecords().get(j).colNameValue.get(btree.colName),
							new Ref(ref.getPage(), j), new Ref(ref.getPage(), j - 1));
				}
			}
			for (RTree rtree : table.getRTreeIndices()) { // deleting the ref of the instance to be removed from the
															// table
				rtree.delete((Comparable) p.tuple.colNameValue.get(rtree.colName), p.ref);
			}
			for (int j = ref.getIndexInPage() + 1; j < page.getSize(); j++) { // shifting all records in the tree
				for (RTree rtree : table.getRTreeIndices()) {
					rtree.updateRef((Comparable) page.getRecords().get(j).colNameValue.get(rtree.colName),
							new Ref(ref.getPage(), j), new Ref(ref.getPage(), j - 1));
				}
			}
			page.getRecords().remove(ref.getIndexInPage());
			if (page.records.isEmpty()) {
				remPage.add(pageAddresses.get(i));
				String newPageName = "classes/DBuggers/" + page.tableName + "_page" + page.pageNumber + ".class";
				File f = new File(newPageName);
				f.delete();
			} else {
				serializePage(page);
			}
		}
		pageAddresses.removeAll(remPage);
		serializeTable(table);
	}

	private static class pair implements Comparable<pair> {
		Tuple tuple;
		Ref ref;

		public pair(Tuple t, Ref r) {
			tuple = t;
			ref = r;
		}

		@Override
		public boolean equals(Object o) {
			pair p = (pair) o;
			return tuple.equals(p.tuple) && ref.equals(p.ref);
		}

		@Override
		public int compareTo(pair o) { 
			return ref.compareTo(o.ref);
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException, DBAppException {

		Table table = (Table) deSerialize(strTableName);
		inputValidity(strTableName, htblColNameValue);
		htblColNameValue = modifyForPolygon(strTableName, htblColNameValue);

		Vector<Vector<Ref>> Union = new Vector<>();
		Hashtable<String, Object> used = new Hashtable<>();
		for (int i = 0; i < table.getBTreeIndices().size(); i++) {
			BPTree curIndex = table.getBTreeIndices().get(i);
			if (htblColNameValue.containsKey(curIndex.colName)) {
				Vector<Ref> refVec = curIndex.search((Comparable) htblColNameValue.get(curIndex.colName));
				if (refVec != null) {
					Union.add(refVec);
					used.put(curIndex.colName, htblColNameValue.get(curIndex.colName));
				}
			}

		}
		for (int i = 0; i < table.getRTreeIndices().size(); i++) {
			RTree curIndex = table.getRTreeIndices().get(i);
			if (htblColNameValue.containsKey(curIndex.colName)) {
				Vector<Ref> refVec = curIndex.search((Comparable) htblColNameValue.get(curIndex.colName));
				if (refVec != null) {
					Union.add(refVec);
					used.put(curIndex.colName, htblColNameValue.get(curIndex.colName));
				}
			}

		}

		Hashtable<String, Object> remaining = new Hashtable<>(); // a hash table that contains the rest of the query not
		// covered by B+ trees
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			if (!used.containsKey(e.getKey())) {
				remaining.put(e.getKey(), e.getValue());
			}
		}
		if (Union.isEmpty()) {
			if (htblColNameValue.containsKey(table.clusteringKeyName)) {
				deleteFromTableClusteringKey(strTableName, htblColNameValue);
			} else
				deleteFromTableUsingNoIndex(strTableName, htblColNameValue);
		} else {
			Vector<Ref> toBeDeleted = new Vector<Ref>(Union.get(0));
			Vector<Ref> toRemainInTBD = new Vector<>(); // the vector that filters toBeDeleted
			for (int i = 1; i < Union.size(); i++) {
				for (int j = 0; j < Union.get(i).size(); j++) {
					if (toBeDeleted.contains(Union.get(i).get(j))) {
						toRemainInTBD.add(Union.get(i).get(j));
					}
				}
				toBeDeleted = new Vector<>(toRemainInTBD);
				toRemainInTBD = new Vector<>();
			}
			Vector<Ref> removedFromTBD = new Vector<>();

			for (Entry<String, Object> e : remaining.entrySet()) {
				for (int i = 0; i < toBeDeleted.size(); i++) {
					Ref curRef = toBeDeleted.get(i);
					Page curPage = (Page) deSerialize(curRef.getPage());
					int idx = curRef.getIndexInPage();
					Tuple tuple = curPage.getRecords().get(idx);
					if (!tuple.colNameValue.get(e.getKey()).equals(e.getValue())) {
						removedFromTBD.add(curRef);
					}
				}
				toBeDeleted.removeAll(removedFromTBD);
				removedFromTBD = new Vector<>();
			}
			Collections.sort(toBeDeleted);

			for (int i = 0; i < toBeDeleted.size(); i++) {
				Ref curRef = toBeDeleted.get(i);
				Page curPage = (Page) deSerialize(curRef.getPage());
				int idx = curRef.getIndexInPage();

				for (BPTree btree : table.getBTreeIndices()) { // removing the instance of the object to be deleted from
					// the B+ tree
					btree.delete((Comparable) curPage.getRecords().get(idx).colNameValue.get(btree.colName), curRef);
				}

				for (int j = idx + 1; j < curPage.getSize(); j++) { // shifting all records in the tree
					for (BPTree btree : table.getBTreeIndices()) {
						btree.updateRef((Comparable) curPage.getRecords().get(j).colNameValue.get(btree.colName),
								new Ref(curRef.getPage(), j), new Ref(curRef.getPage(), j - 1));
					}
				}

				for (RTree rtree : table.getRTreeIndices()) { // removing the instance of the object to be deleted from
					// the R tree
					rtree.delete((Comparable) curPage.getRecords().get(idx).colNameValue.get(rtree.colName), curRef);
				}

				for (int j = idx + 1; j < curPage.getSize(); j++) { // shifting all records in the tree
					for (RTree rtree : table.getRTreeIndices()) {
						rtree.updateRef((Comparable) curPage.getRecords().get(j).colNameValue.get(rtree.colName),
								new Ref(curRef.getPage(), j), new Ref(curRef.getPage(), j - 1));
					}
				}

				curPage.getRecords().remove(idx);
				if (curPage.getRecords().isEmpty()) {

					String newPageName = "classes/DBuggers/" + curPage.tableName + "_page" + curPage.pageNumber
							+ ".class";
					table.pageAddresses.remove(curPage.tableName + "_page" + curPage.pageNumber);
					File f = new File(newPageName);
					f.delete();

				} else {
					serializePage(curPage);

				}

			}
			serializeTable(table);

		}

	}

	private Hashtable<String, Object> modifyForPolygon(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			if (getType(strTableName, e.getKey()).equals("DBuggers.PolygonModified")) {
				htblColNameValue.put(e.getKey(), new PolygonModified((Polygon) e.getValue()));
			}
		}
		return htblColNameValue;

	}

	@SuppressWarnings("unchecked")
	public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[] starrOperators)
			throws ClassNotFoundException, IOException, DBAppException {
		Vector<Tuple> answerToQuery = new Vector<>();
		Vector<Iterator<Ref>> all = new Vector<>();
		for (SQLTerm sql : arrSQLTerms) {
			all.add(solveSingleQuery(sql));
		}
		if (arrSQLTerms.length == 1) {
			while (all.get(0).hasNext()) {
				Ref ref = all.get(0).next();
				Page page = (Page) deSerialize(ref.getPage());
				answerToQuery.add(page.getRecords().get(ref.getIndexInPage()));
			}
			return answerToQuery.iterator();
		}
		Iterator<Ref> collector = null;
		switch (starrOperators[0]) {
		case "OR":
			collector = OR(all.get(0), all.get(1));
			break;
		case "AND":
			collector = AND(all.get(0), all.get(1));
			break;
		default:
			collector = XOR(all.get(0), all.get(1));
			break;
		}
		for (int i = 2; i < all.size(); i++) {
			switch (starrOperators[i - 1]) {
			case "OR":
				collector = OR(all.get(i), collector);
				break;
			case "AND":
				collector = AND(all.get(i), collector);
				break;
			default:
				collector = XOR(all.get(i), collector);
				break;

			}
		}
		while (collector.hasNext()) {
			Ref curRef = collector.next();
			Page page = (Page) deSerialize(curRef.getPage());
			Tuple tuple = page.getRecords().get(curRef.getIndexInPage());
			answerToQuery.add(tuple);
		}

		return answerToQuery.iterator();
	}

	private Iterator<Ref> OR(Iterator<Ref> V1, Iterator<Ref> V2) {
		Vector<Ref> answerToQuery = new Vector<>();
		HashSet<Ref> h = new HashSet<Ref>();
		while (V1.hasNext()) {
			Ref tmp = V1.next();
			answerToQuery.add(tmp);
			h.add(tmp);
		}
		while (V2.hasNext()) {
			Ref tmp = V2.next();
			if (!h.contains(tmp)) {
				answerToQuery.add(tmp);
			}
		}
		return answerToQuery.iterator();
	}

	public Iterator<Ref> AND(Iterator<Ref> V1, Iterator<Ref> V2) {
		Vector<Ref> answerToQuery = new Vector<>();
		HashSet<Ref> h = new HashSet<>();
		while (V1.hasNext()) {
			h.add(V1.next());
		}
		while (V2.hasNext()) {
			Ref tmp = V2.next();
			if (h.contains(tmp)) {
				answerToQuery.add(tmp);
			}
		}
		return answerToQuery.iterator();
	}

	private Iterator<Ref> XOR(Iterator<Ref> V1, Iterator<Ref> V2) {
		Vector<Ref> answerToQuery = new Vector<>();
		Vector<Ref> contentV1 = new Vector<>();
		Vector<Ref> contentV2 = new Vector<>();
		while (V1.hasNext()) {
			contentV1.add(V1.next());
		}
		while (V2.hasNext()) {
			contentV2.add(V2.next());
		}
		Iterator<Ref> and = AND(contentV1.iterator(), contentV2.iterator());
		Iterator<Ref> or = OR(contentV1.iterator(), contentV2.iterator());
		HashSet<Ref> h = new HashSet<>();
		while (and.hasNext()) {
			h.add(and.next());
		}
		while (or.hasNext()) {
			Ref curOr = or.next();
			if (!h.contains(curOr)) {
				answerToQuery.add(curOr);
			}
		}
		return answerToQuery.iterator();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Iterator solveSingleQuery(SQLTerm query) throws ClassNotFoundException, IOException, DBAppException {
		Vector<Ref> answerToQuery = new Vector<>();
		Table table = (Table) deSerialize(query.strTableName);
		if (getType(query.strTableName, query.strColumnName).equals("DBuggers.PolygonModified")) {
			query.objValue = new PolygonModified((Polygon) query.objValue);
		}
		for (BPTree btree : table.getBTreeIndices()) {
			if (btree.colName.equals(query.strColumnName)) {
				switch (query.strOperator) {
				case ">":
					answerToQuery = btree.searchGreater((Comparable) query.objValue);
					break;
				case ">=":
					answerToQuery = btree.searchGreaterEqual((Comparable) query.objValue);
					break;
				case "!=":
					answerToQuery = btree.searchNotEqual((Comparable) query.objValue);
					break;
				case "<":
					answerToQuery = btree.searchSmaller((Comparable) query.objValue);
					break;
				case "<=":
					answerToQuery = btree.searchSmallerEqual((Comparable) query.objValue);
					break;
				case "=":
					answerToQuery = btree.search((Comparable) query.objValue);
					break;
				}
				return answerToQuery.iterator();
			}
		}
		for (RTree rtree : table.getRTreeIndices()) {
			if (rtree.colName.equals(query.strColumnName)) {
				switch (query.strOperator) {
				case ">":
					answerToQuery = rtree.searchGreater((Comparable) query.objValue);
					break;
				case ">=":
					answerToQuery = rtree.searchGreaterEqual((Comparable) query.objValue);
					break;
				case "!=":
					answerToQuery = rtree.searchNotEqual((Comparable) query.objValue);
					break;
				case "<":
					answerToQuery = rtree.searchSmaller((Comparable) query.objValue);
					break;
				case "<=":
					answerToQuery = rtree.searchSmallerEqual((Comparable) query.objValue);
					break;
				case "=":
					answerToQuery = rtree.search((Comparable) query.objValue);
					break;
				}
				return answerToQuery.iterator();
			}
		}
		if (table.getClusteringKey().equals(query.strColumnName)) { // do binary search to increase efficiency of
																	// search
			int pagenum = -1;
			int idx = -1;
			Page page = null;
			TheBiggerPurpose: for (String pageAddress : table.pageAddresses) {
				pagenum++;
				page = (Page) deSerialize(pageAddress);
				int lo = 0;
				int hi = page.getSize() - 1;
				while (lo <= hi) {
					int mid = lo + hi >> 1;
					if (((Comparable) (page.getRecords().get(mid).clusteringKey))
							.compareTo((Comparable) query.objValue) >= 0) {
						idx = mid;
						hi = mid - 1;
					} else {
						lo = mid + 1;
					}
				}
				if (idx != -1) {
					break TheBiggerPurpose;
				}
			}
			if (idx == -1) {
				idx = page.getSize();
			}

			theSwitch: switch (query.strOperator) {
			case "=": {
				for (int i = idx; i < page.getSize(); i++) {
					if (((Comparable) page.getRecords().get(i).clusteringKey)
							.compareTo((Comparable) query.objValue) == 0) {
						answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), i));
					} else {
						return answerToQuery.iterator();
					}
				}
				while (++pagenum < table.pageAddresses.size()) {
					Page p = (Page) deSerialize(table.pageAddresses.get(pagenum));
					for (int j = 0; j < p.getSize(); j++) {
						if (((Comparable) p.getRecords().get(j).clusteringKey)
								.compareTo((Comparable) query.objValue) == 0) {
							answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), j));
						} else {
							return answerToQuery.iterator();
						}
					}
				}
				break theSwitch;

			}
			case ">":
			case ">=": {
				for (int i = idx; i < page.getSize(); i++) {
					boolean cond1 = ((Comparable) page.getRecords().get(i).clusteringKey)
							.compareTo((Comparable) query.objValue) >= 0;
					if (cond1 && query.strOperator.equals(">=")) {
						answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), i));
					} else {
						boolean cond2 = ((Comparable) page.getRecords().get(i).clusteringKey)
								.compareTo((Comparable) query.objValue) > 0;
						if (cond2 && query.strOperator.equals(">")) {

							answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), i));
						}
					}

				}
				while (++pagenum < table.pageAddresses.size()) {
					Page p = (Page) deSerialize(table.pageAddresses.get(pagenum));
					for (int j = 0; j < p.getSize(); j++) {
						boolean cond1 = ((Comparable) p.getRecords().get(j).clusteringKey)
								.compareTo((Comparable) query.objValue) >= 0;
						if (cond1 && query.strOperator.equals(">=")) {

							answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), j));
						} else {
							boolean cond2 = ((Comparable) p.getRecords().get(j).clusteringKey)
									.compareTo((Comparable) query.objValue) > 0;
							if (cond2 && query.strOperator.equals(">")) {

								answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), j));
							}

						}
					}

				}
				return answerToQuery.iterator();
			}

			case "!=": {
				for (String pageAdd : table.pageAddresses) {
					Page p = (Page) deSerialize(pageAdd);
					for (int i = 0; i < p.getSize(); i++) {
						if (((Comparable) p.getRecords().get(i).clusteringKey)
								.compareTo((Comparable) query.objValue) != 0) {
							answerToQuery.add(new Ref(pageAdd, i));
						}
					}

				}
				return answerToQuery.iterator();
			}

			case "<=": {
				int pagenumcopy = pagenum;

				boolean broken = false;
				for (int i = idx; i < page.getSize(); i++) {
					if (((Comparable) page.getRecords().get(i).clusteringKey)
							.compareTo((Comparable) query.objValue) == 0) {
						answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), i));
					} else {
						broken = true;
						break;
					}
				}
				if (!broken)
					while (++pagenumcopy < table.pageAddresses.size()) {
						Page p = (Page) deSerialize(table.pageAddresses.get(pagenumcopy));
						for (int j = 0; j < p.getSize(); j++) {
							if (((Comparable) p.getRecords().get(j).clusteringKey)
									.compareTo((Comparable) query.objValue) == 0) {
								answerToQuery.add(new Ref(table.pageAddresses.get(pagenumcopy), j));
							} else {
								break;
							}
						}
					}
			}
			case "<": {
				for (int i = idx - 1; i >= 0; i--) {
					answerToQuery.add(new Ref(table.pageAddresses.get(pagenum), i));
				}
				for (int i = pagenum - 1; i >= 0; i--) {
					Page p = (Page) deSerialize(table.pageAddresses.get(i));
					for (int j = 0; j < p.getSize(); j++) {
						answerToQuery.add(new Ref(table.pageAddresses.get(i), j));
					}
				}
				return answerToQuery.iterator();
			}

			}

		} else {
			for (String pageAddress : table.pageAddresses) {
				Page page = (Page) deSerialize(pageAddress);
				all: for (int i = 0; i < page.getSize(); i++) {
					switch (query.strOperator) {
					case "=": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) == 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					case "!=": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) != 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					case "<=": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) <= 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					case "<": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) < 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					case ">": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) > 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					case ">=": {
						if (((Comparable) page.getRecords().get(i).getColNameValue().get(query.strColumnName))
								.compareTo(((Comparable) query.objValue)) >= 0) {
							answerToQuery.add(new Ref(pageAddress, i));
						}
						continue all;
					}
					}
				}
			}
		}

		return answerToQuery.iterator();
	}
//	public void removeBtreeIndex(String strTableName,String btreeName) throws ClassNotFoundException, IOException {
//		Table table =(Table) deSerialize(strTableName);
//		BPTree toBeRemoved=null;
//		for(BPTree btree :table.getBTreeIndices()) {
//			if(btree.colName.equals(btreeName)) {
//				toBeRemoved=btree;
//			}
//		}
//		table.getBTreeIndices().remove(toBeRemoved);
//	}

	protected static String getType(String strTableName, String strColName) throws DBAppException, IOException {
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		StringBuilder csvContent = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			csvContent.append(line).append("\n");
		br.close();
		for (String colInfo : csvContent.toString().split("\n")) {

			String[] array = colInfo.split(", *");

			if (!array[0].equals(strTableName))
				continue;

			if (array[1].equals(strColName)) {
				return array[2];
			}

		}
		throw new DBAppException("ERROR: Column \"" + strColName + "\" not found.");

	}

}

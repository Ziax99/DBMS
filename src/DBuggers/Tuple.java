package DBuggers;

import java.awt.Polygon;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

public class Tuple implements Serializable, Comparable<Tuple> {
	Hashtable<String, Object> colNameValue;
	Date TouchDate;

	Object clusteringKey;

	public Tuple(Hashtable<String, Object> data, String tableName) throws IOException, DBAppException {
		TouchDate = new Date();

		colNameValue = new Hashtable<>();

		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		StringBuilder csvContent = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			csvContent.append(line).append("\n");
		br.close();
		for (String colInfo : csvContent.toString().split("\n")) {

			String[] array = colInfo.split(", *");

			if (!array[0].equals(tableName))
				continue;

			if (!data.containsKey(array[1]))
				throw new DBAppException("ERROR: Column \"" + array[1] + "\" not found.");

			Object colVal = data.get(array[1]);
			if (colVal instanceof Polygon) {
				colVal = new PolygonModified((Polygon) colVal);
			}
			Class cls = colVal.getClass();
			String type = cls.getName();
			if (type.equals(array[2])) {
				colNameValue.put(array[1], colVal);
				if (array[3].equals("True"))
					clusteringKey = colVal;
			} else {
				if (!array[2].equals("DBuggers.PolygonModified"))
					throw new DBAppException("Incompatible type: Expected \"" + array[2] + "\" found \"" + (type.equals("DBuggers.PolygonModified")?"java.awt.Polygon":type) + "\"");
				else
					throw new DBAppException(
							"Incompatible type: Expected \"" + "java.awt.Polygon" + "\" found \"" + type + "\"");

			}
		}
	}

	public Hashtable<String, Object> getColNameValue() {
		return colNameValue;
	}

	@Override
	public String toString() {
		String s = "";
		for (Map.Entry<String, Object> e : this.colNameValue.entrySet()) {
			s += e.getKey() + ":" + e.getValue() + ", ";
		}
		s += "TouchDate : " + TouchDate;
		return s;
	}

	@Override
	public int compareTo(Tuple o) {

		Comparable curTuple = (Comparable) clusteringKey;
		Comparable otherTuple = (Comparable) o.clusteringKey;

		return curTuple.compareTo(otherTuple);
	}

	@Override
	public boolean equals(Object o) {
		Tuple tuple = (Tuple) o;
		for (Entry<String, Object> e : this.colNameValue.entrySet()) {
			if (!tuple.getColNameValue().get(e.getKey()).equals(e.getValue())) {
				return false;
			}
		}
		return true;
	}
}

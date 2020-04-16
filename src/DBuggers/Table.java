package DBuggers;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import RTree.RTree;
import bTree.BPTree;

public class Table implements Serializable {

	Vector<String> pageAddresses;
	Vector<BPTree> BTreeIndices;
	Vector<RTree> RTreeIndices;
	String tableName;
	int count;
	String clusteringKeyName;

	public Table(String tableName, String clusteringKeyName) {
		this.tableName = tableName;
		this.clusteringKeyName = clusteringKeyName;
		pageAddresses = new Vector<>();
		BTreeIndices = new Vector<>();
		RTreeIndices = new Vector<>();
	}

	public void add(String newPage) {
		pageAddresses.add(newPage);
	}

	public void remove(String page) {
		pageAddresses.remove(page);
	}
	public Vector<BPTree> getBTreeIndices() {
		return BTreeIndices;
	}

	public void printTable() throws ClassNotFoundException, IOException {
		
		for (String address : pageAddresses) {
		
			Page page = (Page) DBApp.deSerialize(address);
			page.printPage();
		}
		
		for(BPTree tree : BTreeIndices) {
			System.out.println(tree);
			System.out.println("-----------------------------------------------------------");
		}
		
		for (RTree tree : RTreeIndices) {
			System.out.println(tree);
			System.out.println("-----------------------------------------------------------");
		}
	}

	public String getClusteringKey() {
		// TODO Auto-generated method stub
		return clusteringKeyName;
	}

	public Vector<RTree> getRTreeIndices() {
		return RTreeIndices;
	}

}
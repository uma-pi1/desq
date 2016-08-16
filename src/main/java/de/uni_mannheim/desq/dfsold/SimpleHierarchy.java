package de.uni_mannheim.desq.dfsold;


/*
import java.io.FileInputStream;
import java.io.ObjectInputStream;
*/

/**
 * SimpleHierarchy.java
 * 
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class SimpleHierarchy {

	private static SimpleHierarchy instance = null;

	private int[] parentIds;
	
	private int[][] desc;

	// -- Methods

	protected SimpleHierarchy() {

	}

	public static SimpleHierarchy getInstance() {
		if (instance == null) {
			instance = new SimpleHierarchy();
		}
		return instance;
	}

	public void initialize(int[] p) {
		this.parentIds = p;
	}
	
	public void initialize(int[] p, int[][] d) {
		this.parentIds = p;
		this.desc = d;
	}

	/*public void initialize(String fileName) throws Exception {
		FileInputStream fis = new FileInputStream(fileName);
		ObjectInputStream ois = new ObjectInputStream(fis);
		int length = ois.readInt();
		parentIds = new int[length];
		parentIds = (int[]) ois.readObject();
		ois.close();
	}*/

	public int getParent(int item) {
		return parentIds[item];
	}

	public boolean hasParent(int item) {
		return parentIds[item] > 0;
	}

	public int getRoot(int item) {
		while (parentIds[item] != 0) {
			item = parentIds[item];
		}
		return item;
	}

	public boolean isParent(int parent, int child) {
		do {
			if (parent == parentIds[child])
				return true;
			child = parentIds[child];
		} while (child > 0);
		return false;
	}

	public int[] getParents() {
		return parentIds;
	}

	public int[] getDesc(int itemId) {
		return desc[itemId];
	}

	public int size() {
		return parentIds.length;
	}
}

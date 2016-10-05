package old.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.*;
import java.util.Arrays;


/**
 * Dictionary.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class Dictionary {

	private static Dictionary instance = null;

	protected Dictionary() {

	}

	public static Dictionary getInstance() {
		if (instance == null) {
			instance = new Dictionary();
		}
		return instance;
	}

	protected int[] flist;

	protected int[][] parents;

	protected int[][] children;
	
	//TODO: Use String[]
	Int2ObjectOpenHashMap<String> itemIdToItemMap = new Int2ObjectOpenHashMap<>();

	Object2IntOpenHashMap<String> itemToItemIdMap = new Object2IntOpenHashMap<>();
	
	public void load(String fileName) throws IOException {

		BufferedReader br = null;

		FileInputStream fstream = new FileInputStream(fileName);
		DataInputStream in = new DataInputStream(fstream);
		br = new BufferedReader(new InputStreamReader(in));

		ObjectArrayList<int[]> parentList = new ObjectArrayList<>();
		parentList.add(new int[0]);
		
		IntArrayList supportList = new IntArrayList();
		supportList.add(0);
		

		String line = null;

		while ((line = br.readLine()) != null) {
			String[] splits = line.split("\t");
			int itemId = Integer.parseInt(splits[3].trim());
			int itemSupport = Integer.parseInt(splits[2].trim());
			String[] parentsAsString = splits[4].trim().split(",");
			
			// Quick and dirty sanity check
			if (itemId != parentList.size()) {
				System.err.println("Dictionary not sorted");
				System.exit(-1);
			}

			parentList.add(new int[parentsAsString.length]);
			int psize = 0;
			for(String p: parentsAsString ) {
				parentList.get(itemId)[psize++] = Integer.parseInt(p.trim());
			}
			
			supportList.add(itemSupport);

			itemIdToItemMap.put(itemId, splits[0].trim());
			itemToItemIdMap.put(splits[0].trim(), itemId);
		}
		br.close();

		
		parents = new int[parentList.size()][];
		for(int i = 0; i < parentList.size(); ++i) {
			parents[i] = new int[parentList.get(i).length];
			System.arraycopy(parentList.get(i), 0, parents[i], 0, parentList.get(i).length);
			
			if(parents[i].length == 1 && parents[i][0] == 0)
				parents[i] = new int[0];
			
		}
		parentList = null;
		
		flist = new int[supportList.size()];
		supportList.toArray(flist);
		
		supportList = null;
		
		// Compute children
		IntArrayList[] tempChildren = new IntArrayList[parents.length];
		for(int i = 0; i < parents.length; ++i) {
			tempChildren[i] = new IntArrayList();
		}
		
		for(int i = 1; i < parents.length; ++i) {
			for(int j = 0; j < parents[i].length; ++j) {
				tempChildren[parents[i][j]].add(i);
			}
		}
		
		children = new int[tempChildren.length][];
		children[0] = new int[0];
		for(int i = 1; i < children.length; ++i) {
			children[i] = new int[tempChildren[i].size()];
			tempChildren[i].toArray(children[i]);
		}
		tempChildren = null;
	}

	public Int2ObjectOpenHashMap<String> getItemIdToName() {
		return itemIdToItemMap;
	}

	public int[][] getParents() {
		return parents;
	}
	
	public int[] getParents(int itemId) {
		return parents[itemId];
	}

	public int[] getFlist() {
		return flist;
	}
	
	public int[][] getChildren() {
		return children;
	}

	public int getItemId(String itemName) {
		return itemToItemIdMap.getInt(itemName);
	}
	
	public int[] getChildren(int itemId) {
		return children[itemId];
	}
	
	public int[] getDescendants(int itemId) {
		IntOpenHashSet desc = new IntOpenHashSet();
		IntArrayList stack = new IntArrayList();
		stack.add(itemId);
		int top = 0;
		while(top < stack.size()) {
			int descId = stack.getInt(top);
			if(!desc.contains(descId)) {
				desc.add(descId);
				for(int child : getChildren(descId)) {
					stack.add(child);
				}
			}
			top++;
		}
		stack = null;
		return desc.toIntArray();
	}
	
	public int[] getAncestors(int itemId) {
		IntOpenHashSet anc = new IntOpenHashSet();
		IntArrayList stack = new IntArrayList();
		stack.add(itemId);
		int top = 0;
		while(top < stack.size()) {
			int ancId = stack.getInt(top);
			if(!anc.contains(ancId)) {
				anc.add(ancId);
				for(int parent : getParents(ancId)) {
					stack.add(parent);
				}
			}
			top++;
		}
		stack = null;
		return anc.toIntArray();
	}
	
	public int getTotalItems() {
		return flist.length;
	}
	
	public static void main(String[] args) throws IOException {
		Dictionary d = Dictionary.getInstance();
		d.load("/home/kbeedkar/svn/test-data/toy/wc/part-r-00000");
		
		
		System.out.println(Arrays.toString(d.getChildren(1)));
	}
}

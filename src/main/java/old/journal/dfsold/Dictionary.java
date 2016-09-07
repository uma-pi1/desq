package old.journal.dfsold;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


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

	protected int[] fList;

	protected int[] parentIds;

	protected int[][] desc;
	
	Int2ObjectOpenHashMap<String> itemIdToItemMap = new Int2ObjectOpenHashMap<>();

	Object2IntOpenHashMap<String> itemToItemIdMap = new Object2IntOpenHashMap<>();
	
	public void load(String fileName, int minSupport) throws IOException {

		BufferedReader br = null;

		FileInputStream fstream = new FileInputStream(fileName);
		DataInputStream in = new DataInputStream(fstream);
		br = new BufferedReader(new InputStreamReader(in));

		IntArrayList parentList = new IntArrayList();
		parentList.add(0);
		
		IntArrayList supportList = new IntArrayList();
		supportList.add(0);
		

		String line = null;

		while ((line = br.readLine()) != null) {
			String[] splits = line.split("\t");
			int itemId = Integer.parseInt(splits[3].trim());
			int itemSupport = Integer.parseInt(splits[2].trim());
			int parentId = Integer.parseInt(splits[4].trim());

			if (itemId != parentList.size()) {
				System.err.println("Dictionary not sorted");
				System.exit(-1);
			}

			parentList.add(parentId);
			supportList.add(itemSupport);
			

			itemIdToItemMap.put(itemId, splits[0]);
			itemToItemIdMap.put(splits[0], itemId);
		}
		br.close();

		
		parentIds = new int[parentList.size()];
		parentList.toArray(parentIds);
		
		fList = new int[supportList.size()];
		supportList.toArray(fList);
		
		// Precompute descendants
		IntArrayList[] tempDescList = new IntArrayList[parentIds.length];
		for(int i = 0; i < parentIds.length; ++i) {
			tempDescList[i] = new IntArrayList();
		}
		
		for(int i = parentIds.length - 1; i > 0; i--) {
			for(int j = i; j > 0; j = parentIds[j]) {
				tempDescList[j].add(i);
			}
		}
		
		desc = new int[parentIds.length][];
		desc[0] = new int[0];
		for(int i = 1; i < desc.length; ++i) {
			desc[i] = new int[tempDescList[i].size()];
			tempDescList[i].toArray(desc[i]);
		}

	}

	public Int2ObjectOpenHashMap<String> getItemIdToName() {
		return itemIdToItemMap;
	}

	
	public int[] getItemToParent() {
		return parentIds;
	}

	public int[] getFlist() {
		return fList;
	}
	
	public int[][] getDesc() {
		return desc;
	}

	public int getItemId(String itemName) {
		return itemToItemIdMap.getInt(itemName);
	}
	public static void main(String[] args) throws IOException {
	}

}

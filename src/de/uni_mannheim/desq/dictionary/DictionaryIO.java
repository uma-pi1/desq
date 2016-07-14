package de.uni_mannheim.desq.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/* I/O method for Dictionary */
public class DictionaryIO {
	/** Adds an item to the specified dictionary by deconding a line from del file format. */
	public static void addItemFromDelLine(Dictionary dict, String line) {
		// create the item
		String[] columns = line.split("\t");
		String label = columns[0];
		int id = Integer.parseInt(columns[3]);
		Item item = new Item(id, label);
		item.fid = item.id;
		item.cFreq = Integer.parseInt(columns[1]);
		item.dFreq = Integer.parseInt(columns[2]);
		dict.addItem(item);
		
		// add parents
		String[] parents = columns[4].split(",");
		for (String parentString : parents) {
			int parentId = Integer.parseInt(parentString);
			if (parentId == 0) continue; // indicates no parent
			Item parent = dict.getItemById(parentId);
			Item.addParent(item, parent);
		}
	}
	
	/** Load dictionary from del file format.
	 * 
	 * label <TAB> cFreq <TAB> dFreq <TAB> id <TAB> comma-separated parent ids
	 * 
	 */
	public static Dictionary loadFromDel(InputStream in) throws IOException {
		Dictionary dict = new Dictionary();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		while ((line = br.readLine()) != null) {
			addItemFromDelLine(dict, line);
		}
		br.close();
		return dict;
	}
	
	/** Format an item in del file format */
	public static String itemToDelLine(Item item) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.label);
		sb.append("\t");
		sb.append(Integer.toString(item.cFreq));
		sb.append("\t");
		sb.append(Integer.toString(item.dFreq));
		sb.append("\t");
		sb.append(Integer.toString(item.fid));
		sb.append("\t");
		if (item.parents.isEmpty()) {
			sb.append("0");
		} else {
			String sep = "";
			for (Item parent : item.parents) {
				sb.append(sep);
				sb.append(Integer.toString(parent.fid));
				sep = ",";
			}
		}
		return sb.toString();
	}
	
	/** Save a dictionary to del line format */
	public static void saveToDel(OutputStream out, Dictionary dict) throws IOException {
		List<Integer> fids = new ArrayList<Integer>(dict.itemsByFid.keySet());
		Collections.sort(fids);
		
		OutputStreamWriter writer = new OutputStreamWriter(out);
		for (Integer fid : fids) {
			Item item = dict.getItemByFid(fid);
			writer.write(itemToDelLine(item));
			writer.write("\n");
		}
		writer.close();
	}
}

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
	public static void addItemFromDelLine(Dictionary dict, String line, boolean withStatistics) {
		// create the item
		String[] columns = line.split("\t");
		Item item = null;
		String[] parents = null;
		if (withStatistics) {
			String sid = columns[0];
			int id = Integer.parseInt(columns[3]);
			item = new Item(id, sid);
			item.fid = item.id;
			item.cFreq = Integer.parseInt(columns[1]);
			item.dFreq = Integer.parseInt(columns[2]);
			dict.addItem(item);
			parents = columns[4].split(",");
		} else {
			String sid = columns[0];
			int id = Integer.parseInt(columns[1]);
			item = new Item(id, sid);
			item.fid = -1;
			item.cFreq = 0;
			item.dFreq = 0;
			dict.addItem(item);
			parents = columns[2].split(",");
		}
		
		// add parents
		for (String parentString : parents) {
			int parentId = Integer.parseInt(parentString);
			if (parentId == 0) continue; // indicates no parent
			Item parent = dict.getItemById(parentId);
			Item.addParent(item, parent);
		}
	}
	
	/** Load dictionary from del file format.
	 * 
	 * with statistics:
	 * sid <TAB> cFreq <TAB> dFreq <TAB> id (equals fid) <TAB> comma-separated parent ids
	 * 
	 * without statistics:
	 * sid <TAB> id <TAB> comma-separated parent ids
	 * 
	 */
	public static Dictionary loadFromDel(InputStream in, boolean withStatistics) throws IOException {
		Dictionary dict = new Dictionary();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		while ((line = br.readLine()) != null) {
			addItemFromDelLine(dict, line, withStatistics);
		}
		br.close();
		return dict;
	}
	
	/** Format an item in del file format */
	public static String itemToDelLine(Item item, boolean withStatistics) {
		StringBuilder sb = new StringBuilder();
		sb.append(item.sid);
		sb.append("\t");
		if (withStatistics) {
			sb.append(Integer.toString(item.cFreq));
			sb.append("\t");
			sb.append(Integer.toString(item.dFreq));
			sb.append("\t");
			sb.append(Integer.toString(item.fid));
		} else {
			sb.append(Integer.toString(item.id));
		}
		sb.append("\t");
		if (item.parents.isEmpty()) {
			sb.append("0");
		} else {
			String sep = "";
			for (Item parent : item.parents) {
				sb.append(sep);
				sb.append(Integer.toString(withStatistics ? parent.fid : parent.id));
				sep = ",";
			}
		}
		return sb.toString();
	}
	
	/** Save a dictionary to del line format. If withStatitics is set, uses fids, else uses ids. */
	public static void saveToDel(OutputStream out, Dictionary dict, boolean withStatistics) throws IOException {
		List<Integer> items = new ArrayList<Integer>(
				withStatistics ? dict.itemsByFid.keySet() : dict.itemsById.keySet());
		Collections.sort(items);
		
		OutputStreamWriter writer = new OutputStreamWriter(out);
		for (Integer i : items) {
			Item item = withStatistics ? dict.getItemByFid(i) : dict.getItemById(i);
			writer.write(itemToDelLine(item, withStatistics));
			writer.write("\n");
		}
		writer.flush();
	}
}

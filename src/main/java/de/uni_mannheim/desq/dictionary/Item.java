package de.uni_mannheim.desq.dictionary;

import java.util.*;

/** A single item in a dictionary. */
public class Item {
	public int id;
	public String sid;

	public int fid = -1;
	public int cFreq = -1;
	public int dFreq = -1;
	public List<Item> children = new ArrayList<>();
	public List<Item> parents = new ArrayList<>();
	public Properties properties;
	
	public Item(int id, String sid) {
		this.id = id;
		this.sid = sid;
	}
	
	/** Connects child and parent. Modifies child.parents and parent.children. */ 
	public static void addParent(Item child, Item parent) {
		child.parents.add(parent);
		parent.children.add(child);
	}
	
	public String toString() {
		return sid;
	}
	
	/** Returns a copy of this item but does not copy childs and parents and shares the properties */
	public Item shallowCopyWithoutEdges() {
		Item item = new Item(id, sid);
		item.fid = fid;
		item.cFreq = cFreq;
		item.dFreq = dFreq;
		item.properties = properties;
		return item;
	}
	
	public static Comparator<Item> dfreqDecrComparator() {
		return new Comparator<Item>() {
			@Override
			public int compare(Item o1, Item o2) {
				return o2.dFreq - o1.dFreq;
			}
		};
	}
}

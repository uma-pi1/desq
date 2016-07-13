package de.uni_mannheim.desq.dictionary;

import java.util.ArrayList;
import java.util.List;

/** A single item in a dictionary. */
public class Item {
	public int id;
	public String label;

	public int fid = -1;
	public int cFreq = -1;
	public int dFreq = -1;
	public List<Item> children = new ArrayList<Item>();
	public List<Item> parents = new ArrayList<Item>();
	
	public Item(int id, String label) {
		this.id = id;
		this.label = label;
	}
	
	/** Connects child and parent. Modifies child.parents and parent.children. */ 
	public static void addParent(Item child, Item parent) {
		child.parents.add(parent);
		parent.children.add(child);
	}
	
	public String toString() {
		return label;
	}
	
	/** Returns a copy of this item but does not copy childs and parents */
	public Item copyWithoutEdges() {
		Item item = new Item(id, label);
		item.fid = fid;
		item.cFreq = cFreq;
		item.dFreq = dFreq;
		return item;
	}
}

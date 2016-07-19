package de.uni_mannheim.desq.dictionary;

import java.util.*;

/** A single item in a dictionary.  */
public class Item {
	/** Stable global identifier of this item */
	public int gid;

	/** Unique name of this item */
	public String sid;

	/** Internal "frequency" identifier of this item used to support efficient mining. The identifier is not
	 * necessarily stable. Generally, the frequency identifier needs to satisfy certain properties, which depend
	 * on the actual miner being used. In most cases, these properties match the ones described in
	 * {@link Dictionary#recomputeFids()}}. */
	public int fid = -1;

	/** Collection frequency of this item and its ascendants */
	public int cFreq = -1;

    /** Document frequency of this item and its ascendants */
    public int dFreq = -1;

    /** Children of this item */
	public List<Item> children = new ArrayList<>();

    /** Parents of this item. */
    public List<Item> parents = new ArrayList<>();

    /** Other properties associated with this item */
    public Properties properties;
	
	public Item(int id, String sid) {
		this.gid = id;
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
		Item item = new Item(gid, sid);
		item.fid = fid;
		item.cFreq = cFreq;
		item.dFreq = dFreq;
		item.properties = properties;
		return item;
	}

	/** Returns a comparator that compares by {@link de.uni_mannheim.desq.dictionary.Item#dFreq} descending. */
	public static Comparator<Item> dfreqDecrComparator() {
		return (o1, o2) -> o2.dFreq - o1.dFreq;
	}
}

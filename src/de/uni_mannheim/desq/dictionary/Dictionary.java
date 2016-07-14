package de.uni_mannheim.desq.dictionary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.uni_mannheim.desq.utils.IntSetUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

/** A set of items arranged in a hierarchy */ 
public class Dictionary {
	// indexes
	Map<Integer, Item> itemsById = new HashMap<Integer, Item>();
	Map<Integer, Item> itemsByFid = new HashMap<Integer, Item>();
	Map<String, Item> itemsBySid = new HashMap<String, Item>();
	
	// -- updating the hierarchy ------------------------------------------------------------------
	
	/** Adds a new item to the hierarchy. If the fid equals 0, it won't be indexed */
	public void addItem(Item item) {
		if (containsId(item.id)) {
			throw new IllegalArgumentException("Item id '" + item.id + "' exists already");
		}
		if (itemsBySid.containsKey(item.sid)) {
			throw new IllegalArgumentException("Item sid '" + item.sid + "' exists already");
		}
		if (item.fid >= 0 && itemsByFid.containsKey(item.fid)) {
			throw new IllegalArgumentException("Item fid '" + item.id + "' exists already");
		}
		itemsById.put(item.id, item);
		if (item.fid >= 0) itemsByFid.put(item.fid, item);
		itemsBySid.put(item.sid, item);		
	}
	
	
	// -- access to indexes -----------------------------------------------------------------------
	
	public Collection<Item> allItems() {
		return itemsById.values();
	}
	
	/** Checks whether there is an item with the given ID in the hierarchy. */
	public boolean containsId(int itemId) {
		return itemsById.containsKey(itemId);
	}
	
	/** Returns the item with the given id (or null if no such item exists) */
	public Item getItemById(int itemId) {
		return itemsById.get(itemId);
	}
	
	/** Checks whether there is an item with the given FID in the hierarchy. */
	public boolean containsFid(int itemFid) {
		return itemsByFid.containsKey(itemFid);
	}
	
	/** Returns the item with the given fid (or null if no such item exists) */
	public Item getItemByFid(int itemFid) {
		return itemsByFid.get(itemFid);
	}
	
	/** Returns all items for the given fids */
	public List<Item> getItemsByFids(IntSet itemFids) {
		List<Item> items = new ArrayList<Item>();
		getItemsByFids(itemFids, items);
		return items;
	}
	
	/** Stores all items for the given fids in the target list */
	public void getItemsByFids(IntSet itemFids, List<Item> target) {
		target.clear();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			target.add(getItemByFid(it.nextInt()));
		}		
	}
	
	/** Checks whether there is an item with the given sid in the hierarchy. */
	public boolean containsSid(String itemSid) {
		return itemsBySid.containsKey(itemSid);
	}
	
	/** Returns the item with the given sid (or null if no such item exists) */
	public Item getItemBySid(String itemSid) {
		return itemsBySid.get(itemSid);
	}
	
	
	// -- computing descendants and ascendants ----------------------------------------------------
	
	/** Returns the fids of all descendants of the given item (including the given item) */
	public IntSet descendantsFids(int itemFid) {
		return descendantsFids(IntSets.singleton(itemFid));		
	}
	
	/** Returns the fids of all descendants of the given items (including the given items) */
	public IntSet descendantsFids(IntSet itemFids) {
		IntSet descendants = new IntOpenHashSet();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			if (!descendants.contains(itemFid)) {
				descendants.add(itemFid);
				addDescendants(getItemByFid(itemFid), descendants);
			}
		}
		return IntSetUtils.optimize(descendants);
	}
	
	/** Adds all descendants of the specified item to itemFids, excluding the given item and all 
	 * descendants of items already present in itemFids. */	
	public void addDescendants(Item item, IntSet itemFids) {
		for (Item child : item.children) {
			if (!itemFids.contains(child.fid)) {
				itemFids.add(child.fid);
				addDescendants(getItemByFid(child.fid), itemFids);
			}
		}
	}

	/** Returns the fids of all ascendants of the given item (including the given item) */
	public IntSet ascendantsFids(int itemFid) {
		return ascendantsFids(IntSets.singleton(itemFid));		
	}

	/** Returns the fids of all ascendants of the given items (including the given items) */
	public IntSet ascendantsFids(IntSet itemFids) {
		IntSet ascendants = new IntOpenHashSet();
		IntIterator it = itemFids.iterator();
		while (it.hasNext()) {
			int itemFid = it.nextInt();
			if (!ascendants.contains(itemFid)) {
				ascendants.add(itemFid);
				addAscendants(getItemByFid(itemFid), ascendants);
			}
		}
		return ascendants;
	}
	
	/** Adds all ascendants of the specified item to itemFids, excluding the given item and all 
	 * ascendants of items already present in itemFids. */	
	public void addAscendants(Item item, IntSet itemFids) {
		for (Item parent : item.parents) {
			if (!itemFids.contains(parent.fid)) {
				itemFids.add(parent.fid);
				addAscendants(getItemByFid(parent.fid), itemFids);
			}
		}
	}
	
	/** Returns a copy of this dictionary that contains only the specified items (including
	 * the *direct* links between these items).
	 * 
	 * TODO: also add indirect links? (takes some thought to figure out which links to acutally add and how to do this 
	 * reasonably efficiently; perhaps helpful: a method that removes "unnecessary" links)
	 */
	public Dictionary restrictedCopy(IntSet itemFids) {
		// copy the relevant items
		Dictionary dict = new Dictionary();
		for (Item item : itemsByFid.values()) {
			if (!itemFids.contains(item.fid)) continue;
			Item copiedItem = item.shallowCopyWithoutEdges();
			dict.addItem(copiedItem);
		}
		
		// add indirect links
		for (Item copiedItem : dict.itemsById.values()) {
			Item item = getItemByFid(copiedItem.fid);
			for (Item child : item.children) {
				if (itemFids.contains(child.fid)) {
					Item copiedChild = dict.getItemByFid(child.fid);
					Item.addParent(copiedChild, copiedItem);
				}
			}
		}
		
		return dict;
	}
	
	/** Returns array a where a[i] is document frequency of item with fid i. Be careful when fids
	 * are sparse; the resulting array might then get big */
	public IntList getFlist() {
		IntList flist = new IntArrayList();
		flist.size(itemsByFid.size()+1);
		for(Entry<Integer, Item> entry : itemsByFid.entrySet()) {
			int fid = entry.getKey().intValue();
			if (fid>flist.size()) flist.size(fid+1);
			flist.set(fid, entry.getValue().dFreq);
		}
		return flist;
	}
}

package sandbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.DictionaryIO;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

public class DictionaryExample {
	static void nyt() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data-local/nyt-1991-dict.del"), true);
		
		Item item;
		IntSet fids;

		// compute ascendants
		item = dict.getItemBySid("was@be@VB@");
		System.out.println(item);
		fids = dict.ascendantsFids(item.fid);
		System.out.println("Asc: " + dict.getItemsByFids(fids));

		// compute descendants
		item = dict.getItemBySid("be@VB@");
		System.out.println(item);
		fids = dict.descendantsFids(item.fid);
		System.out.println("Desc: " + dict.getItemsByFids(fids));

		// restrict the dictionary to specified subset
		Dictionary restricted = dict.restrictedCopy(
				dict.descendantsFids(dict.getItemBySid("DT@").fid));
		DictionaryIO.saveToDel(System.out, restricted, true);
		
	}
	
	static void icdm16() throws IOException {
		// load the dictionary
		Dictionary dict = DictionaryIO.loadFromDel(
				new FileInputStream("data/icdm16/example-dict.del"), false);
		System.out.println("All items: " + dict.allItems());
		
		// print data
		System.out.println("Input sequences:");
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data/icdm16/example-data.del"));
		IntList inputSequence = new IntArrayList();
		while (dataReader.read(inputSequence)) {
			List<Item> items = new ArrayList<Item>();
			for (int id : inputSequence) {
				items.add(dict.getItemById(id));
			}
			System.out.println(items);
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		//nyt();
		icdm16();
	}
}

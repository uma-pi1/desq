package de.uni_mannheim.desq.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.DelSequenceWriter;
import de.uni_mannheim.desq.io.SequenceReader;
import de.uni_mannheim.desq.io.SequenceWriter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

public class DictionaryExample {
	static void nyt() throws IOException {
		// use to convert old file to new format
		//Dictionary dict = DictionaryIO.loadFromDel(new FileInputStream("data-local/nyt-1991-dict.del"), true);
		//dict.write("data-local/nyt-1991-dict.json");
		//dict.write("data-local/nyt-1991-dict.avro.gz");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz");

		Item item;
		IntSet fids;

		// compute ascendants
		//item = dict.getItemBySid("was@be@VB@");
		item = dict.getItemBySid("vice@NN@");
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
		restricted.writeJson(System.out);
		System.out.println();
	
		// clear the counts in the dictionary and recopute
		System.out.println("Recomputing counts");
		System.out.println(dict.getItemBySid("be@VB@").toJson());
		dict.clearCountsAndFids();
		System.out.println(dict.getItemBySid("be@VB@").toJson());
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data-local/nyt-1991-data.del"), false);
		dict.incCounts(dataReader);
		System.out.println(dict.getItemBySid("be@VB@").toJson());
	}

	static void icdm16() throws IOException {
		URL dictFile = DictionaryExample.class.getResource("/icdm16-example/dict.json");
		URL dataFile = DictionaryExample.class.getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom(dictFile);
		System.out.println("All items: " + dict.getItems());


		// print data
		System.out.println("Input sequences:");
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsIds(inputSequence)) {
			System.out.println(dict.getItemsByIds(inputSequence));
		}
		
		// update hierarchy
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incCounts(dataReader);
		System.out.println("Dictionary with counts: ");
		dict.writeJson(System.out);
		System.out.println();

		// update fids
		System.out.println("Dictionary with new fids: ");
		dict.recomputeFids();
		dict.writeJson(System.out);
		System.out.println();

		// show converted input sequences
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		SequenceWriter dataWriter = new DelSequenceWriter(System.out, false);
		while (dataReader.readAsIds(inputSequence)) {
			dict.gidsToFids(inputSequence);
			dataWriter.write(inputSequence);
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		//nyt();
		icdm16();
	}
}

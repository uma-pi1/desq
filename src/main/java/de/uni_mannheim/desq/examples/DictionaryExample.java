package de.uni_mannheim.desq.examples;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.io.DelSequenceReader;
import de.uni_mannheim.desq.io.SequenceReader;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

public class DictionaryExample {
	static void nyt() throws IOException {
		// load the dictionary
		Dictionary dict = Dictionary.loadFrom("data-local/nyt-1991-dict.avro.gz");

		int fid;
		IntSet fids;

		// compute ascendants
		fid = dict.fidOf("vice@NN@");
		System.out.println(dict.toJson(fid));
		fids = dict.ascendantsFids(fid);
		System.out.println("Asc: " + dict.sidsOfFids(fids));

		// compute descendants
		fid = dict.fidOf("be@VB@");
		System.out.println(dict.toJson(fid));
		fids = dict.descendantsFids(fid);
		System.out.println("Desc: " + dict.sidsOfFids(fids));

		// restrict the dictionary to specified subset
		//RestrictedDictionary restricted = new RestrictedDictionary(dict, dict.descendantsFids(dict.fidOf("DT@")));
		// restricted.writeJson(System.out);
		//System.out.println();
	
		// clear the counts in the dictionary and recopute
		System.out.println("Recomputing counts");
		System.out.println(dict.toJson(dict.fidOf("be@VB@")));
		dict.clearFreqs();
		System.out.println(dict.toJson(dict.fidOf("be@VB@")));
		SequenceReader dataReader = new DelSequenceReader(
				new FileInputStream("data-local/nyt-1991-data.del"), false);
		dict.incFreqs(dataReader);
		System.out.println(dict.toJson(dict.fidOf("be@VB@")));
	}

	static void icdm16() throws IOException {
		URL dictFile = DictionaryExample.class.getResource("/icdm16-example/dict.json");
		URL dataFile = DictionaryExample.class.getResource("/icdm16-example/data.del");

		// load the dictionary
		Dictionary dict = Dictionary.loadFrom(dictFile);
		System.out.println("All items: " + dict.sids());
		System.out.println("isForest: " + dict.isForest());
		System.out.println();

		// print data
		System.out.println("Input sequences (gids->sids):");
		SequenceReader dataReader = new DelSequenceReader(dataFile.openStream(), false);
		IntList inputSequence = new IntArrayList();
		while (dataReader.readAsGids(inputSequence)) {
			System.out.println(dict.sidsOfGids(inputSequence));
		}
		System.out.println();

		// update hierarchy
		dataReader = new DelSequenceReader(dataFile.openStream(), false);
		dict.incFreqs(dataReader);
		System.out.println("Dictionary with counts: ");
		dict.writeJson(System.out);
		System.out.println();
		System.out.println("hasConsistentFids: " + dict.hasConsistentFids());
		System.out.println();

		// update fids
		System.out.println("Dictionary with new fids: ");
		dict.recomputeFids();
		dict.writeJson(System.out);
		System.out.println();
		System.out.println();

		// show converted input sequences
		System.out.println("Converted sequences (gids->new fids->sids): ");
		dataReader = new DelSequenceReader(dict, dataFile.openStream(), false);
		while (dataReader.readAsFids(inputSequence)) {
			System.out.println(dict.sidsOfFids(inputSequence));
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		//nyt();
		icdm16();
	}
}

package de.uni_mannheim.desq.io;

import de.uni_mannheim.desq.dictionary.Dictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DelSequenceReader extends SequenceReader {
	private final BufferedReader reader;
	private final boolean usesFids;

	public DelSequenceReader(Dictionary dict, InputStream in, boolean usesFids) {
		this.dict = dict;
		this.reader = new BufferedReader(new InputStreamReader(in));
		this.usesFids = usesFids;
	}

	public DelSequenceReader(InputStream in, boolean usesFids) {
		this(null, in, usesFids);
	}

		@Override
	public boolean read(IntList items) throws IOException {
		String line = reader.readLine();
		if (line == null) {
			items.clear();
			return false;
		}
		parseLine(line, items);
		return true;
	}

	@Override
	public boolean usesFids() {
		return usesFids;
	}

	public static void parseLine(String line, IntList items) {
		String[] tokens = line.split("[\t ]");
		items.size(tokens.length);
		items.clear();
		for (String token : tokens) {
			if (token.isEmpty()) continue;
			items.add(Integer.parseInt(token));
		}
	}

	public static IntList parseLine(String line) {
		String[] tokens = line.split("[\t ]");
		IntList items = new IntArrayList(tokens.length);
		for (String token : tokens) {
			if (token.isEmpty()) continue;
			items.add(Integer.parseInt(token));
		}
		return items;
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

}

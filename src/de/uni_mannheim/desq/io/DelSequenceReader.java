package de.uni_mannheim.desq.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import it.unimi.dsi.fastutil.ints.IntList;

public class DelSequenceReader extends SequenceReader {
	private BufferedReader reader;

	public DelSequenceReader(InputStream in) {
		this.reader = new BufferedReader(new InputStreamReader(in));
	}

	@Override
	public boolean read(IntList itemFids) throws IOException {
		itemFids.clear();
		
		String line = reader.readLine();
		if (line == null) return false;
		for (String token : line.split("[\t ]")) {
			itemFids.add(Integer.parseInt(token));
		}
		return true;
	}	

}

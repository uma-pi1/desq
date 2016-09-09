package de.uni_mannheim.desq.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import it.unimi.dsi.fastutil.ints.IntList;

public class DelSequenceReader extends SequenceReader {
	private final BufferedReader reader;
	private final boolean usesFids;

	public DelSequenceReader(InputStream in, boolean usesFids) {
		this.reader = new BufferedReader(new InputStreamReader(in));
		this.usesFids = usesFids;
	}

	@Override
	public boolean read(IntList itemFids) throws IOException {
		itemFids.clear();
		
		String line = reader.readLine();
		if (line == null) return false;
		for (String token : line.split("[\t ]")) {
			if (token.isEmpty()) continue;
			itemFids.add(Integer.parseInt(token));
		}
		return true;
	}

	@Override
	public boolean usesFids() {
		return usesFids;
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

}

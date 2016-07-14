package de.uni_mannheim.desq.io;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

import it.unimi.dsi.fastutil.ints.IntList;

public class DelPatternWriter extends PatternWriter {
	private PrintWriter writer;
	private boolean convertToIds;

	private DelPatternWriter(PrintWriter writer, boolean convertToIds) {
		this.writer = writer;
		this.convertToIds = convertToIds;
	}

	private DelPatternWriter(Writer writer, boolean convertToIds) {
		this(new PrintWriter(writer, true), convertToIds);
	}	
	
	private DelPatternWriter(OutputStream out, boolean convertToIds) {
		this(new PrintWriter(out, true), convertToIds);
	}

	@Override
	public void write(IntList itemFids, long count) {
		writer.print(count);
		writer.print("\t");
		for (int i=0; i<itemFids.size(); i++) {
			int fid = itemFids.getInt(i);
			if (convertToIds) {
				writer.print( dict.getItemByFid(fid).id );					
			} else {
				writer.print( fid );
			}
		}
		writer.println();
	}

	public void close() {
		writer.close();
	}
}

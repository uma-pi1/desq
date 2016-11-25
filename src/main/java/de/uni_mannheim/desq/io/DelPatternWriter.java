package de.uni_mannheim.desq.io;

import it.unimi.dsi.fastutil.ints.IntList;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

public class DelPatternWriter extends PatternWriter {
	public enum TYPE { FID, GID, SID }

	private final PrintWriter writer;
	private final TYPE type;

	public DelPatternWriter(PrintWriter writer, TYPE type) {
		this.writer = writer;
		this.type = type;
	}

	public DelPatternWriter(Writer writer, TYPE type) {
		this(new PrintWriter(writer, true), type);
	}	
	
	public DelPatternWriter(OutputStream out, TYPE type) {
		this(new PrintWriter(out, true), type);
	}

	@Override
	public void write(IntList itemFids, long frequency) {
		writer.print(frequency);
		writer.print("\t");
		String sep="";
		for (int i=0; i<itemFids.size(); i++) {
			int fid = itemFids.getInt(i);
			writer.write(sep);
			sep = "\t";
			switch (type) {
				case FID:
					writer.print( fid );
					break;
				case GID:
					writer.print( dict.gidOf(fid));
					break;
				case SID:
					writer.print( dict.sidOfFid(fid));
					break;
			}
		}
		writer.println();
	}

	public void close() {
		writer.close();
	}
}

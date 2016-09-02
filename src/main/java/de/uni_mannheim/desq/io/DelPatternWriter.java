package de.uni_mannheim.desq.io;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

import it.unimi.dsi.fastutil.ints.IntList;

public class DelPatternWriter extends PatternWriter {
	public static enum TYPE { FID, GID, SID };

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
					writer.print( dict.getItemByFid(fid).gid);
					break;
				case SID:
					writer.print( dict.getItemByFid(fid).sid);
					break;
			}
		}
		writer.println();
	}

	@Override
	public void writeReverse(IntList reverseItemFids, long frequency) {
		writer.print(frequency);
		writer.print("\t");
		String sep="";
		for (int i=reverseItemFids.size()-1; i>=0; i--) {
			int fid = reverseItemFids.getInt(i);
			writer.write(sep);
			sep = "\t";
			switch (type) {
				case FID:
					writer.print( fid );
					break;
				case GID:
					writer.print( dict.getItemByFid(fid).gid);
					break;
				case SID:
					writer.print( dict.getItemByFid(fid).sid);
					break;
			}
		}
		writer.println();
	}

	public void close() {
		writer.close();
	}
}

package de.uni_mannheim.desq.collector;

import java.io.DataOutput;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

import it.unimi.dsi.fastutil.ints.IntList;

/** Collects patterns in del format: count <TAB> comma-separated list of ids or fids */
public class DelPatternCollector extends PatternCollector {
	private PrintWriter writer;
	private boolean convertToIds;

	private DelPatternCollector(PrintWriter writer, boolean convertToIds) {
		this.writer = writer;
		this.convertToIds = convertToIds;
	}

	private DelPatternCollector(Writer writer, boolean convertToIds) {
		this(new PrintWriter(writer, true), convertToIds);
	}	
	
	private DelPatternCollector(OutputStream out, boolean convertToIds) {
		this(new PrintWriter(out, true), convertToIds);
	}

	@Override
	public void collect(IntList itemFids, long count) {
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

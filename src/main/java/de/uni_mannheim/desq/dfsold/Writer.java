package de.uni_mannheim.desq.dfsold;

import java.io.IOException;

public interface Writer {
	void write(int[] sequence, long count) throws IOException, InterruptedException;
}

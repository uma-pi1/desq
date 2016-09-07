package old.writer;

import java.io.IOException;

// TODO: remove; superceded by PatternCollector
public interface Writer {
	void write(int[] sequence, long count) throws IOException, InterruptedException;
}

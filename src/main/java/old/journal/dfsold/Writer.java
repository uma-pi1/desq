package old.journal.dfsold;

import java.io.IOException;

public interface Writer {
	void write(int[] sequence, long count) throws IOException, InterruptedException;
}

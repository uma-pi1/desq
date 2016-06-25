package writer;

import java.io.IOException;

public interface Writer {
	void write(int[] sequence, double count) throws IOException, InterruptedException;
}

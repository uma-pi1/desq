package mining.scores;

import java.io.IOException;

public interface RankedScoreList {
	
	public void addNewOutputSequence(int[] transaction, double score);
	public double getMinScore();
	public void printList() throws IOException, InterruptedException;
}

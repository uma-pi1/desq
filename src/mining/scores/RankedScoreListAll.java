package mining.scores;

import java.io.IOException;
import java.util.ArrayList;

import writer.SequentialWriter;

public class RankedScoreListAll implements RankedScoreList {

	boolean sortBeforeOutput;
	ArrayList<RankItem> outputSequences = new ArrayList<RankItem>();
	
	public RankedScoreListAll(boolean sortBeforeOutput) {
		this.sortBeforeOutput = sortBeforeOutput;
	}
	
	@Override
	public void addNewOutputSequence(int[] sequence, double score) {
		int[] seq = new int[sequence.length];
		System.arraycopy(sequence, 0, seq, 0, sequence.length);
		outputSequences.add(new RankItem(seq, score));
	}

	@Override
	public double getMinScore() {
		return Double.MIN_VALUE;
	}

	@Override
	public void printList() throws IOException, InterruptedException {
		SequentialWriter writer = SequentialWriter.getInstance();
		
		if(this.sortBeforeOutput) {
			outputSequences.sort(RankItem.scoreComparator);
		}
		
		int[][] sequences = new int[outputSequences.size()][];
		double[] scores = new double[outputSequences.size()];
		
		int arrayIndex = 0;
		for (RankItem rankItem : outputSequences) {
			sequences[arrayIndex] = rankItem.sequence;
			scores[arrayIndex] = rankItem.score;
			
			arrayIndex++;
			
		}
		writer.writeAll(sequences, scores);
		
	}
}

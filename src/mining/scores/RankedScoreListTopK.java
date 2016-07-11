package mining.scores;

import java.io.IOException;
import java.util.Comparator;

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import writer.SequentialWriter;

public class RankedScoreListTopK implements RankedScoreList {

	Comparator<RankItem> scoreComparator = new Comparator<RankItem>() {
        @Override public int compare(RankItem o1, RankItem o2) {
            return o1.compareTo(o2);
        }           
    };
	
    ObjectAVLTreeSet<RankItem> outputSequences = new ObjectAVLTreeSet<RankItem>(scoreComparator);
    
	double minScore;
	int maxTopK;
	
	public RankedScoreListTopK(int maxTopK) {
		this.maxTopK = maxTopK;
		this.minScore = Double.MIN_VALUE;
	}
	
	
	@Override
	public void addNewOutputSequence(int[] sequence, double score) {
		int[] seq = new int[sequence.length];
		System.arraycopy(sequence, 0, seq, 0, sequence.length);
		
		if(score > minScore && outputSequences.size() <= maxTopK) {
			outputSequences.add(new RankItem(seq, score));
		}
		
		if(score > minScore && outputSequences.size() > maxTopK) {
			outputSequences.add(new RankItem(seq, score));
			outputSequences.remove(outputSequences.last());
		}
		
		if(score == minScore) {
			outputSequences.add(new RankItem(seq, score));
		}
		
		if(outputSequences.size() >= maxTopK) {
			minScore = outputSequences.last().score;
		}
	}

	@Override
	public double getMinScore() {
		return minScore;
	}

	@Override
	public void printList() throws IOException, InterruptedException {
		System.out.println("Print List");
		SequentialWriter writer = SequentialWriter.getInstance();
		
		int[][] sequences = new int[outputSequences.size()][];
		double[] scores = new double[outputSequences.size()];
		long[] support = new long[outputSequences.size()]; 
		
		int arrayIndex = 0;
		for (RankItem rankItem : outputSequences) {
			sequences[arrayIndex] = rankItem.sequence;
			scores[arrayIndex] = rankItem.score;
			
			arrayIndex++;
			
		}
		writer.writeAll(sequences, scores);
		
	}
}

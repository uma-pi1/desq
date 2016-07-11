package mining.scores;

import java.util.Comparator;


public class RankItem implements Comparable<RankItem> {
	double score;
	int[] sequence;
	
	public static Comparator<RankItem> scoreComparator = new Comparator<RankItem>() {
	    @Override public int compare(RankItem o1, RankItem o2) {
	        return o1.compareTo(o2);
	    }           
	};
	
	public RankItem(int[] sequence, double score) {
		this.score = score;
		this.sequence = sequence;
	}

	@Override
	public int compareTo(RankItem o) {
		return Double.compare(this.score, o.score) * -1;
	}
	
	
}



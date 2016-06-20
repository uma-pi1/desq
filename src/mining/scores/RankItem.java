package mining.scores;

import java.util.Comparator;


public class RankItem implements Comparable<RankItem> {
	double score;
	int support;
	int[] sequence;
	
	public static Comparator<RankItem> scoreComparator = new Comparator<RankItem>() {
	    @Override public int compare(RankItem o1, RankItem o2) {
	        return o1.compareTo(o2);
	    }           
	};
	
	public RankItem(int[] sequence, double score, int support) {
		this.score = score;
		this.support = support;
		this.sequence = sequence;
	}

	@Override
	public int compareTo(RankItem o) {
		return Double.compare(this.score, o.score) * -1;
	}
	
	
}



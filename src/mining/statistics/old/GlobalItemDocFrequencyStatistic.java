package mining.statistics.old;

import utils.Dictionary;

public class GlobalItemDocFrequencyStatistic {
	
	protected int[] flist;
	
	public GlobalItemDocFrequencyStatistic() {
		flist = Dictionary.getInstance().getFlist();
	}
	
	public int getFrequency(int item) {
		return flist[item];
	}
}

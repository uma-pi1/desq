package mining.interestingness;

import fst.XFst;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mining.scores.DesqCountScore;
import mining.scores.RankedScoreList;
import mining.statistics.collectors.DesqGlobalDataCollector;
import mining.statistics.collectors.DesqProjDbDataCollector;
import mining.statistics.data.ProjDbStatData;
import utils.Dictionary;
import utils.IntArrayStrategy;
import utils.PrimitiveUtils;
import writer.SequentialWriter;
import driver.DesqConfig.Match;


/**
 * DesqCount.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqCountScored {

	protected Dictionary dictionary = Dictionary.getInstance();

	protected Object2LongOpenCustomHashMap<int[]> outputSequences = new Object2LongOpenCustomHashMap<int[]>(
			new IntArrayStrategy());

	protected double sigma;

	protected XFst xfst;

	protected boolean writeOutput = true;
	
	protected int[] sequence;

	protected int sid = -1;
	
	protected int numPatterns = 0;
	
	protected int[] flist = Dictionary.getInstance().getFlist();

	protected SequentialWriter writer = SequentialWriter.getInstance();
	
	protected long gpt = 0L;
	
	protected long gptUnique = 0L;
	
	protected long globalGpt = 0L;
	
	protected  long globalGptUnique = 0L;
	
	protected  long totalMatchedSequences = 0L;
	
	protected  long totalMatches = 0L;

	protected Match match;
	
	protected DesqCountScore score;
	
	RankedScoreList rankedScoreList;
	
	protected HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?,?>, ?>> globalDataCollectors;
	protected HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> projDbCollectors;
	ProjDbStatData projDbStatData = new ProjDbStatData();
	
	protected Object2ObjectOpenCustomHashMap<int[], HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> > outputSequenceList = new Object2ObjectOpenCustomHashMap<int[],HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>>(new IntArrayStrategy());
	
	// Methods
	
	public DesqCountScored(double sigma, XFst dfa, DesqCountScore score,  HashMap<String, DesqGlobalDataCollector<? extends DesqGlobalDataCollector<?, ?>, ?>> globalDataCollectors, RankedScoreList rankedScoreList, boolean writeOutput,  Match match) {
		this.sigma = sigma;
		this.xfst = dfa;
		this.writeOutput = writeOutput;
		this.match = match;
		this.score = score;
		
		this.projDbCollectors = score.getProjDbCollectors();
		this.globalDataCollectors = globalDataCollectors;
		
		this.rankedScoreList = rankedScoreList;
	}
	
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		IntArrayList buffer = new IntArrayList();
		
		String line;
		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] str = line.split(" ");
				sequence = new int[str.length];
				for (int i = 0; i < str.length; ++i) {
					sequence[i] = Integer.parseInt(str[i]);
				}
				
				sid = sid + 1;
				gpt = 0;
				gptUnique = 0;
				buffer.clear();
				//computeMatch(buffer, 0, dfa.getInitialState());
				computeMatch();
				globalGpt += gpt;
				globalGptUnique += gptUnique;
				
				if(gpt > 0)
					totalMatchedSequences++;
			}
		}
		br.close();
		
		totalMatches = outputSequences.size();

		// output all frequent sequences
//		for (Map.Entry<int[], Long> entry : outputSequences.entrySet()) {
//			long value = entry.getValue();
//			int support = PrimitiveUtils.getLeft(value);
////			if (score.getScore(entry.getKey(), collectors, support) >= sigma) {
//			if (score.getScoreByProjDb(entry.getKey(), globalDataCollectors, finalStateProjDbCollectors) >= sigma) {
//				numPatterns++;
//				if(writeOutput) {
//					rankedScoreList.addNewOutputSequence(entry.getKey(), score.getScore(entry.getKey(), collectors, support), support);
////					writer.write(entry.getKey(), support);
//				}
//			}
//		}
		
		for (Map.Entry<int[], HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>> entry : outputSequenceList.entrySet()) {
			
			HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> finalCollectors = entry.getValue();
			if (score.getScoreByProjDb(entry.getKey(), globalDataCollectors, finalCollectors) >= sigma) {
				numPatterns++;
				if(writeOutput) {
					rankedScoreList.addNewOutputSequence(entry.getKey(), score.getScoreByProjDb(entry.getKey(), globalDataCollectors, finalCollectors), 0);
				}
			}
		}
	}
	
	protected abstract void computeMatch();

	protected void countSequence(int[] sequence) {
		gpt++;
		Long supSid = outputSequences.get(sequence);
		if (supSid == null) {
			outputSequences.put(sequence, PrimitiveUtils.combine(1, sid));
			gptUnique++;
			return;
		}
		if (PrimitiveUtils.getRight(supSid) != sid) {
			int newCount = PrimitiveUtils.getLeft(supSid) + 1;
			outputSequences.put(sequence, PrimitiveUtils.combine(newCount, sid));
			gptUnique++;
		}
	}
	
	protected void updateFinalSequenceStatistics(int[] sequence) {
		HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> finalStateProjDbAccumulators = outputSequenceList.get(sequence);
		if (finalStateProjDbAccumulators == null) {
			finalStateProjDbAccumulators = new HashMap<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>>();
			for (Entry<String, DesqProjDbDataCollector<? extends DesqProjDbDataCollector<?, ?>, ?>> entry: projDbCollectors.entrySet()) {
				@SuppressWarnings("unchecked")
				DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> coll = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) entry.getValue();
				finalStateProjDbAccumulators.put(entry.getKey(), coll.supplier().get());
			}

		} 
		
		projDbStatData.setPosition(-1);
		projDbStatData.setStateFST(-1);
		projDbStatData.setTransaction(this.sequence);
		projDbStatData.setTransactionId(this.sid);
		projDbStatData.setInFinalState(true);
		
		for (Entry<String, DesqProjDbDataCollector<?, ?>> entry : finalStateProjDbAccumulators.entrySet()) {
			// at compile time it is not decided which type the accept function 
			@SuppressWarnings("unchecked")
			DesqProjDbDataCollector<DesqProjDbDataCollector<?,?>, ?> finalProjDBCollector = (DesqProjDbDataCollector<DesqProjDbDataCollector<?, ?>, ?>) finalStateProjDbAccumulators.get(entry.getKey());
			finalProjDBCollector.accumulator().accept(entry.getValue(), projDbStatData);
		}		
		outputSequenceList.put(sequence, finalStateProjDbAccumulators);
	}


	public long getGlobalGpt() {
		return globalGpt;
	}


	public long getTotalMatchedSequences() {
		return totalMatchedSequences;
	}


	public long getGlobalGptUnique() {
		return globalGptUnique;
	}


	public int noOutputPatterns() {
		return numPatterns;
	}


	public long getTotalMatches() {
		return totalMatches;
	}
	
//	private final class Prefix {
//		
//		int[] prefix;
//		
//		Prefix(int[] prefix) {
//			this.prefix = prefix;
//		}
//
//		@Override
//		public int hashCode() {
//			final int prime = 31;
//			int result = 1;
//			result = prime * result + getOuterType().hashCode();
//			result = prime * result + Arrays.hashCode(prefix);
//			return result;
//		}
//
//		@Override
//		public boolean equals(Object obj) {
//			if (this == obj)
//				return true;
//			if (obj == null)
//				return false;
//			if (getClass() != obj.getClass())
//				return false;
//			Prefix other = (Prefix) obj;
//			if (!getOuterType().equals(other.getOuterType()))
//				return false;
//			if (!Arrays.equals(prefix, other.prefix))
//				return false;
//			return true;
//		}
//
//		private DesqCountScored getOuterType() {
//			return DesqCountScored.this;
//		}
//	}

}

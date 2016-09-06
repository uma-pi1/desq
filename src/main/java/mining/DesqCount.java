package mining;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import de.uni_mannheim.desq.util.IntArrayStrategy;
import driver.DesqConfig.Match;
import fst.XFst;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenCustomHashMap;
import utils.Dictionary;
import utils.PrimitiveUtils;
import writer.DelWriter;


/**
 * DesqCount.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqCount {

	protected Dictionary dictionary = Dictionary.getInstance();

	protected Object2LongOpenCustomHashMap<int[]> outputSequences = new Object2LongOpenCustomHashMap<>(
			new IntArrayStrategy());

	protected int sigma;

	protected XFst xfst;

	protected boolean writeOutput = true;
	
	protected boolean useFlist = true;
	
	protected int[] sequence;

	protected int sid = -1;
	
	protected int numPatterns = 0;
	
	protected int[] flist = Dictionary.getInstance().getFlist();

	protected DelWriter writer = DelWriter.getInstance();
	
	protected long gpt = 0L;
	
	protected long gptUnique = 0L;
	
	protected long globalGpt = 0L;
	
	protected  long globalGptUnique = 0L;
	
	protected  long totalMatchedSequences = 0L;
	
	protected  long totalMatches = 0L;

	protected Match match;
	
	// Methods
	
	public DesqCount(int sigma, XFst dfa, boolean writeOutput, boolean useFlist, Match match) {
		this.sigma = sigma;
		this.xfst = dfa;
		this.writeOutput = writeOutput;
		this.useFlist = useFlist;
		this.match = match;
	}
	
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line;
		while ((line = br.readLine()) != null) {
			if (!line.isEmpty()) {
				String[] str = line.split("\\s* \\s*");
				sequence = new int[str.length];
				for (int i = 0; i < str.length; ++i) {
					sequence[i] = Integer.parseInt(str[i]);
				}
				
				sid = sid + 1;
				gpt = 0;
				gptUnique = 0;
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
		for (Map.Entry<int[], Long> entry : outputSequences.entrySet()) {
			long value = entry.getValue();
			int support = PrimitiveUtils.getLeft(value);
			if (support >= sigma) {
				numPatterns++;
				if(writeOutput) {
					writer.write(entry.getKey(), support);
				}
			}
		}
	}
	
	protected abstract void computeMatch();
	
	protected void countSequence(int[] sequence) {
		gpt++;
		Long supSid = outputSequences.getLong(sequence);
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
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

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

}

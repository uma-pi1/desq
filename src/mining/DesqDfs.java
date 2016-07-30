package mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.BitSet;

import fst.XFst;
import utils.Dictionary;
import writer.SequentialWriter;

/**
 * DesqDfs.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public abstract class DesqDfs {

	protected Dictionary dictionary = Dictionary.getInstance();
	
	protected ArrayList<int[]> inputSequences = new ArrayList<int[]>();
	
	protected static int sId;
	
	protected XFst xfst;
	
	protected int sigma;
	
	protected SequentialWriter writer = SequentialWriter.getInstance();
	
	protected int[] flist = Dictionary.getInstance().getFlist();
	
	protected int numPatterns = 0;
	
	protected boolean writeOutput = true;
	
	// Methods
	
	public DesqDfs() {}
	
	public DesqDfs(int sigma, XFst xfst, boolean writeOutput) {
		this.sigma = sigma;
		this.xfst = xfst;
		this.writeOutput = writeOutput;
	}
	
	public void clear() {
		inputSequences.clear();
	}
	
	public void scan(String file) throws Exception {
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		
		while((line = br.readLine()) != null) {
			if(!line.isEmpty()) {
				String[] rawSequence = line.split(" ");
				int[] inputSequence = new int[rawSequence.length];
				int i = 0;
				for (String s : rawSequence) {
					try {
						inputSequence[i++] = Integer.parseInt(s);
					} catch (NumberFormatException e) {
				        System.out.println(rawSequence + " " + inputSequence.length); 
				    }
				}
				addInputSequence(inputSequence);
			}
		}
		br.close();
	}
	
	protected abstract void addInputSequence(int[] inputSequence);

	public abstract void mine() throws IOException, InterruptedException;
	
	public int noPatterns(){
		return numPatterns;
	}
	
}

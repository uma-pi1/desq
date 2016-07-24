package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
//import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
//import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Iterator;
import java.util.Properties;

import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.ItemState;
import de.uni_mannheim.desq.patex.PatEx;
import de.uni_mannheim.desq.util.PropertiesUtils;

/**
 * CompressedDesqDfs.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class CompressedDesqDfs extends CompressedMemoryDesqMiner {

	// parameters for mining
	String patternExpression;
	long sigma;
	
	// helper variables
	Fst fst;
	int initialStateId;
	int largestFrequentFid;
	boolean reachedFinalState;
	final NewPostingList.Iterator postingsIt = new NewPostingList.Iterator();
	final NewPostingList.Iterator inputIt = inputSequences.iterator();
	
	public CompressedDesqDfs(DesqMinerContext ctx) {
		super(ctx);
		inputOffsets = new IntArrayList();
		this.patternExpression = PropertiesUtils.get(ctx.properties, "patternExpression");
		this.sigma = PropertiesUtils.getLong(ctx.properties, "minSupport");
		
		patternExpression = ".* [" + patternExpression.trim() + "]";
		PatEx p = new PatEx(patternExpression, ctx.dict);
		this.fst = p.translate();
		fst.minimize();//TODO: move to translate
		
		this.largestFrequentFid = ctx.dict.getLargestFidAboveDfreq(sigma);
		this.initialStateId = fst.getInitialState().getId();
	}

	
	public static Properties createProperties(String patternExpression, int sigma) {
		Properties properties = new Properties();
		PropertiesUtils.set(properties, "patternExpression", patternExpression);
		PropertiesUtils.set(properties, "minSupport", sigma);
		return properties;
	}
	
	@Override
	public void clear() {
		super.clear();
	}

	@Override
	public void mine() {
		if(inputSupports.size() >= sigma) {
			
			DesqDfsTreeNode root = new DesqDfsTreeNode(new DesqDfsProjectedDatabase(fst.numStates()));
			
			for(int inputId = 0; inputId < inputSupports.size(); inputId++) {
				int inputOffset = inputOffsets.getInt(inputId);
				inputIt.offset = inputOffset;
				int inputSupport = inputSupports.get(inputId);
				incStep(inputId, inputOffset, inputSupport, initialStateId, root);
			}
			
			root.expansionsToChildren(sigma);
			expand(new IntArrayList(), root);
			root.clear();
		}
	}
	
	
	private void expand(IntList prefix, DesqDfsTreeNode node) {
		// add a placeholder to prefix
        int lastPrefixIndex = prefix.size();
        prefix.add(-1);
        
        // We do not need this in DESQ DFS (see below)
        //IntSet childrenToNotExpand = new IntOpenHashSet();
        
        // iterate over children
        for(DesqDfsTreeNode childNode : node.children )  {
        	
        	//p-support
        	long support = 0;
        	
        	// we first expand and then output
        	DesqDfsProjectedDatabase projectedDatabase = childNode.projectedDatabase;
        	assert projectedDatabase.prefixSupport >= sigma;
        	prefix.set(lastPrefixIndex, projectedDatabase.itemFid);
        	
        	
        	// This pruning does not work with DESQ DFS
        	// TODO: double check
        	// Ex; input sequence=ab, PE=([a=b|A=^c])
        	/*if(childrenToNotExpand.contains(projectedDatabase.itemFid)) {
        		childNode.clear();
        		continue;
        	}*/
        	
        	// do the expansion
        	postingsIt.reset(projectedDatabase.postingList);
        	do{
        		int inputId = postingsIt.nextNonNegativeInt();
        		int inputOffset = inputOffsets.getInt(inputId);
        		int inputSupport = inputSupports.getInt(inputId);
        		
        		reachedFinalState = false;
        		
        		//For all state@pos for a sequence
        		do {
        			int stateId = postingsIt.nextNonNegativeInt(); //stateId
        			inputIt.offset = inputOffset + postingsIt.nextNonNegativeInt(); // position of input item
        			
        			incStep(inputId,inputOffset, inputSupport, stateId, childNode);
        		} while(postingsIt.hasNext());
        		
        		if(reachedFinalState) {
        			support += inputSupport;
        		}
        	} while(postingsIt.nextPosting());
        	
        	// output if p-frequent
        	if(support >= sigma) {
        		if(ctx.patternWriter != null) {
        			ctx.patternWriter.write(prefix, support);
        		}
        	}
        	
        	
        	childNode.expansionsToChildren(sigma);
        	// This pruning does not work with DESQ DFS (see above)
        	/*if(childNode.children.isEmpty()) {
                ctx.dict.addDescendantFids(ctx.dict.getItemByFid(projectedDatabase.itemFid), childrenToNotExpand);
        	}*/
        	
        	childNode.projectedDatabase.clear();
        	expand(prefix, childNode);
        	childNode.clear();
        }
        
        prefix.removeInt(lastPrefixIndex);
        
        
		
	}

	
	private void incStep(int inputId, int inputOffset, int inputSupport, int stateId, DesqDfsTreeNode node) {
		reachedFinalState |= fst.getState(stateId).isFinal();
		if(!inputIt.hasNext())
			return;
		int itemFid = inputIt.nextInt();
		int nextItemOffset = inputIt.offset-inputOffset;
		
		//TODO: reuse iterators!
        Iterator<ItemState> itemStateIt = fst.getState(stateId).consume(itemFid);
        while(itemStateIt.hasNext()) {
        	ItemState itemState = itemStateIt.next();
        	int outputItemFid = itemState.itemFid;
        	
        	int toStateId = itemState.state.getId();
        	if(outputItemFid == 0) { //EPS output
        		incStep(inputId, inputOffset, inputSupport, toStateId, node);
        	} else {
        		if(largestFrequentFid >= outputItemFid) {
        			node.expandWithItem(outputItemFid, inputId, inputSupport, nextItemOffset, toStateId);
        		}
        	}
        }
		
	}
	

}

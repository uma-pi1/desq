package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExParser.*;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.log4j.Logger;

import java.util.*;


public final class PatExToFst {
	private static final Logger logger = Logger.getLogger(PatExToFst.class);
	String expression;
	BasicDictionary dict;
	Map<String,Transition> transitionCache = new HashMap<>(); // caches transition
	boolean optimizeRepeats;

	/** If the pattern expression contains string item identifiers, the dict needs to be of type {@link Dictionary}.
	 *
	 * @param optimizeRepeats if true, the FST is optimized before any repeat experssion (e.g., {0,10) is used.
	 *                           Can save substantial computational cost for large FSTs.
	 */
	public PatExToFst(String expression, BasicDictionary dict, boolean optimizeRepeats) {
		this.expression = expression;
		this.dict = dict;
		this.optimizeRepeats = optimizeRepeats;
	}

	public PatExToFst(String expression, BasicDictionary dict) {
		this(expression, dict, true);
	}

	public Fst translate(){
		return translate(false);
	}

	public Fst translate(boolean itemsetPatEx) {
		transitionCache.clear();
		ANTLRInputStream input = new ANTLRInputStream(expression);

		// Lexer
		PatExLexer lexer = new PatExLexer(input);

		// Tokens
		CommonTokenStream tokens = new CommonTokenStream(lexer);

		// Parser
		PatExParser parser = new PatExParser(tokens);

		// Parse tree
		ParseTree tree = parser.patex();

		// Visitor for parse tree
		Visitor visitor = new Visitor(itemsetPatEx);

		// Create FST from the syntax tree
		Fst fst = visitor.visit(tree);

		fst.updateStates();
		fst.optimize();

		if (fst.getFinalStates().isEmpty() || !fst.hasOutput()) {
			logger.warn("FST has no transitions that can produce outputs. Did you forget to add capture groups? " +
					"Pattern expression: " + expression);
		}

		//fst.exportGraphViz("complete.pdf");

		return fst;
	}

	public class Visitor extends PatExBaseVisitor<Fst> {

		private static final String unorderedMarker = "!";
		private boolean capture = false;
		private boolean unordered = false;  //Flag if within unordered symbol <...>
		private int unorderedConcatId = -1; //unordered: currently active concat ID (handover in concatExpression)
		private int maxConcatId = -1; //unordered: watermark for easy generation of new concat id
		private ArrayList<HashMap<Fst,int[]>> unorderedConcatElements = new ArrayList<>(); //unordered: elements of concat with frequencies
		private boolean itemsetPatEx = false; //Flag: true if target dataset is itemset

		public Visitor(boolean itemsetPatEx){
			super();
			this.itemsetPatEx = itemsetPatEx;
		}
		
		@Override
		public Fst visitUnion(UnionContext ctx) {
			Fst fst = visit(ctx.unionexp());

			fst.updateStates();

			// add self loop to starting state if we can start anywhere
			if (ctx.start == null) {
				State initialState = fst.getInitialState();
				initialState.addTransition(
						TransitionFactory.uncapturedDot(dict, initialState)
				);
			}

			// add new self-loop final state if we can end anywhere
			if (ctx.end == null) {
				State newFinalState = new State(true);
				newFinalState.addTransition(
						TransitionFactory.uncapturedDot(dict, newFinalState)
				);
				for(State finalState : fst.getFinalStates()) {
					finalState.simulateEpsilonTransitionTo(newFinalState);
				}
			}

			return fst;
			//return visit(ctx.unionexp());
		}

		
		@Override
		public Fst visitUnionExpression(UnionExpressionContext ctx) {
			return FstOperations.union(visit(ctx.concatexp()), visit(ctx.unionexp()));
		}

		
		@Override
		public Fst visitConcat(ConcatContext ctx) {
			return visit(ctx.getChild(0));
		}

		
		@Override
		public Fst visitConcatExpression(ConcatExpressionContext ctx) {
			//only call if pattern "repeatexp concatexp" matches (actual concatination)
			if(unorderedConcatId < 0 && (unordered || itemsetPatEx)){
				//no active "unordered" concatenation but unordered flag set -> new "unordered" concatenation
				unordered = false; //stop inheritance
				unorderedConcatId = ++maxConcatId;

				unorderedConcatElements.add(unorderedConcatId, new HashMap<>());
			}

			if(unorderedConcatId > -1){
				//remember unorderedConcatId locally
				int localConcatId = unorderedConcatId;
				//add current step/fst to backlog (mitght be added by the nested repeatexp() call already)
				unorderedConcatElements.get(localConcatId).putIfAbsent(visit(ctx.repeatexp()),null);
				//iterate further in concatenation, and handover unorderedConcatId to next concat step (was un-set by child to avoid inheritance)
				unorderedConcatId = localConcatId;
				return visit(ctx.concatexp());
			}else {
				return FstOperations.concatenate(visit(ctx.repeatexp()), visit(ctx.concatexp()));
			}
		}

		
		@Override
		public Fst visitRepeatExpression(RepeatExpressionContext ctx) {
			//only called for last element of concatexp (end of recursion, if there was one)
			if(unorderedConcatId > -1) {
				//remember concat processing id locally
				int localConcatId = unorderedConcatId;
				//add last element of concat to list
				unorderedConcatElements.get(localConcatId).putIfAbsent(visit(ctx.repeatexp()),null);
				/*//optional: create pdfs to trace fsts of the concat
				int idx = 0;
				for (Fst fst: concatElements.get(localConcatId)){
					fst.exportGraphViz("unorderedList_" + localConcatId + "_" + idx + ".pdf");
					idx++;
				}*/
				//Permute all combinations: E1E2...En
				Fst union = FstOperations.permute(unorderedConcatElements.get(localConcatId));
				//Clear backlog
				unorderedConcatElements.get(localConcatId).clear();
				//return union of Fst permutations (results of concatexp)
				return union;
			}else {
				return visit(ctx.repeatexp());
			}
		}

		
		@Override
		public Fst visitOptionalExpression(OptionalExpressionContext ctx) {
			if(unorderedConcatId > -1) unorderedConcatId = -1;
			return FstOperations.optional(visit(ctx.repeatexp()));
		}

		
		@Override
		public Fst visitRepeatMinMaxExpression(RepeatMinMaxExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT(0).getText());
			int max = Integer.parseInt(ctx.INT(1).getText());
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(min,max,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				if (optimizeRepeats) fst.optimize();
				return FstOperations.repeatMinMax(fst, min, max);
			}
		}



		@Override
		public Fst visitRepeatExactlyExpression(RepeatExactlyExpressionContext ctx) {
			int n = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(n,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				if (optimizeRepeats) fst.optimize();
				return FstOperations.repeatExactly(fst, n);
			}
		}
		
		@Override
		public Fst visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
			int max = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(0,max,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				if (optimizeRepeats) fst.optimize();
				return FstOperations.repeatMinMax(fst, 0, max);
			}
		}

		
		@Override
		public Fst visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(min,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				if (optimizeRepeats) fst.optimize();
				return FstOperations.repeatMin(fst, min);
			}
		}

		
		@Override
		public Fst visitSimpleExpression(SimpleExpressionContext ctx) {
			if (unorderedConcatId > -1) unorderedConcatId = -1; //stop inheritance
			return visit(ctx.simpleexp());
		}

		
		@Override
		public Fst visitPlusExpression(PlusExpressionContext ctx) {
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(1,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				if (optimizeRepeats) fst.optimize();
				return FstOperations.plus(fst);
			}
		}

		
		@Override
		public Fst visitStarExpression(StarExpressionContext ctx) {
			//Check if unordered marker exists
			boolean isUnordered = ctx.getChild(1).getText().equals(unorderedMarker);
			if(unorderedConcatId > -1 && (itemsetPatEx || isUnordered)){ //direct child of unordered concatenation
				return handleUnorderedRepeat(0,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				Fst fst = visit(ctx.repeatexp());
				return FstOperations.kleene(fst);
			}
		}
		
		@Override
		public Fst visitItemExpression(ItemExpressionContext ctx) {
			if(itemsetPatEx) {
				return FstOperations.concatenate(visit(ctx.itemexp()),getDotKleene());
			}else{
				return visit(ctx.itemexp());
			}
		}
		
		@Override
		public Fst visitParens(ParensContext ctx) {
			return visit(ctx.unionexp());
		}

		
		@Override
		public Fst visitCapture(CaptureContext ctx) {
			capture = true;
			Fst nfa = visit(ctx.unionexp());
			capture = false;
			return nfa;
		}

		
		@Override
		public Fst visitWildCard(WildCardContext ctx) {
			boolean generalize = false;
			if (ctx.getChildCount() > 1) {
				// operator generalize
				generalize = true;
			}

			Fst fst = new Fst();
			Transition t;
			if(capture) {
				if (generalize)
					t = TransitionFactory.capturedGeneralizedDot(dict, new State(true));
				else
					t = TransitionFactory.capturedDot(dict, new State(true));
			} else {
				t = TransitionFactory.uncapturedDot(dict, new State(true));
			}
			fst.getInitialState().addTransition(t);
			fst.updateStates();
			return fst;
		}

		
		@Override
		public Fst visitNonWildCard(NonWildCardContext ctx) {
			String label = ctx.item().getText();
			int fid = PatExUtils.asFid(dict, ctx.item());
			boolean generalize = false;
			boolean force = false;
			int opCount = ctx.getChildCount();
			if (opCount == 2) {
				// either A= or A^
				force = ctx.getChild(1).getText().equals("=");
				generalize = ctx.getChild(1).getText().equals("^");
			} else if (opCount == 3) {
				// A=^
				force = true;
				generalize = true;
			}

			// build the two-state FST
			Fst fst = new Fst();

			// see if we have a cached transition
			String transitionKey = Integer.toString(fid) + force + generalize + capture;
			Transition t;
			if (transitionCache.containsKey(transitionKey)) {
				// if so, , use a shallow copy of this one (to share data structures)
				Transition cachedT = transitionCache.get(transitionKey);
				t = cachedT.shallowCopy();
				t.setToState(new State(true));
				//System.out.println(transitionKey);
			} else {
				// otherwise compute it
				if (capture) {
					if (force && generalize) { // case: A=^
						t = TransitionFactory.capturedConstant(dict, new State(true), fid, label);
					} else if (force & !generalize) { // case A=
						t = TransitionFactory.capturedItem(dict, new State(true), fid, label, false);
					} else if (!force && generalize) { // case A^
						t = TransitionFactory.capturedGeneralizedItem(dict, new State(true), fid, label);
					} else { // case A
						t = TransitionFactory.capturedItem(dict, new State(true), fid, label, true);
					}
				} else {
					assert !generalize;
					t = TransitionFactory.uncapturedItem(dict, new State(true), fid, label, !force);
				}

				transitionCache.put(transitionKey, t); // remember for reuse
			}
			fst.getInitialState().addTransition(t);
			fst.updateStates();
			return fst;
		}
		@Override
		public Fst visitUnordered(UnorderedContext ctx) {
			unordered = true;
			if (itemsetPatEx) {
				logger.warn("Unordered group <...> used for itemset data -> Treated like group [...]! " +
						"Pattern expression: " + expression);
			}
			return visit(ctx.concatexp()); //children should remove unordered flag (no inheritance to all children)
		}

		private Fst getDotKleene() {
			Fst fst = new Fst();
			Transition t = TransitionFactory.uncapturedDot(dict, new State(true));
			fst.getInitialState().addTransition(t);
			fst.updateStates();
			return FstOperations.kleene(fst);
		}

		private Fst handleUnorderedRepeat(Integer n, Integer m, RepeatexpContext visit){
			int localConcatId = unorderedConcatId; // remember id locally
			unorderedConcatId = -1; //stop inheritance
			Fst fst = visit(visit);
			if (optimizeRepeats) fst.optimize();
			int[] freq = {(n != null) ? n : 0, (m != null) ? m : 0}; //frequencies handled during concatenation
			unorderedConcatElements.get(localConcatId).put(fst,freq);
			return fst;
		}

		private void addUnorderedWarning(){
			logger.warn("Unordered frequency !{n,m} used outside of unordered <...> -> Treated like sequential '{n,m}'! " +
					"Pattern expression: " + expression);
		}
	}
}

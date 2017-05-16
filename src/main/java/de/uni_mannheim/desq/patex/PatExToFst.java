package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.patex.PatExParser.*;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


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

	public Fst translate() {
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
		Visitor visitor = new Visitor();

		// Create FST from the syntax tree
		Fst fst = visitor.visit(tree);

		fst.updateStates();
		fst.optimize();

		if (fst.getFinalStates().isEmpty() || !fst.hasOutput()) {
			logger.warn("FST has no transitions that can produce outputs. Did you forget to add capture groups? " +
					"Pattern expression: " + expression);
		}

		return fst;
	}

	public class Visitor extends PatExBaseVisitor<Fst> {
		
		private boolean capture = false;
		
		
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
			return FstOperations.concatenate(visit(ctx.repeatexp()), visit(ctx.concatexp()));
		}

		
		@Override
		public Fst visitRepeatExpression(RepeatExpressionContext ctx) {
			return visit(ctx.repeatexp());
		}

		
		@Override
		public Fst visitOptionalExpression(OptionalExpressionContext ctx) {
			return FstOperations.optional(visit(ctx.repeatexp()));
		}

		
		@Override
		public Fst visitRepeatMinMaxExpression(RepeatMinMaxExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT(0).getText());
			int max = Integer.parseInt(ctx.INT(1).getText());
			Fst fst = visit(ctx.repeatexp());
			if (optimizeRepeats) fst.optimize();
			return FstOperations.repeatMinMax(fst, min, max);
		}


		@Override
		public Fst visitRepeatExactlyExpression(RepeatExactlyExpressionContext ctx) {
			int n = Integer.parseInt(ctx.INT().getText());
			Fst fst = visit(ctx.repeatexp());
			if (optimizeRepeats) fst.optimize();
			return FstOperations.repeatExactly(fst, n);
		}
		
		@Override
		public Fst visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
			int max = Integer.parseInt(ctx.INT().getText());
			Fst fst = visit(ctx.repeatexp());
			if (optimizeRepeats) fst.optimize();
			return FstOperations.repeatMinMax(fst, 0, max);
		}

		
		@Override
		public Fst visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT().getText());
			Fst fst = visit(ctx.repeatexp());
			if (optimizeRepeats) fst.optimize();
			return FstOperations.repeatMin(fst, min);
		}

		
		@Override
		public Fst visitSimpleExpression(SimpleExpressionContext ctx) {
			return visit(ctx.simpleexp());
		}

		
		@Override
		public Fst visitPlusExpression(PlusExpressionContext ctx) {
			Fst fst = visit(ctx.repeatexp());
			if (optimizeRepeats) fst.optimize();
			return FstOperations.plus(fst);
		}

		
		@Override
		public Fst visitStarExpression(StarExpressionContext ctx) {
			Fst fst = visit(ctx.repeatexp());
			return FstOperations.kleene(fst);
		}

		
		@Override
		public Fst visitItemExpression(ItemExpressionContext ctx) {
			return visit(ctx.itemexp());
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
	}
}

package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.dictionary.Item;
import de.uni_mannheim.desq.fst.*;
import de.uni_mannheim.desq.fst.BasicTransition.InputLabelType;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;
import de.uni_mannheim.desq.patex.PatExParser.*;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;


public final class PatEx {
	String expression;
	Dictionary dict;
	
	public PatEx(String expression, Dictionary dict) {
		this.expression = expression;
		this.dict = dict;
	}
	
	public Fst translate() {
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
						new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.EPSILON, initialState, dict)
				);
			}

			// remember whether we need to match to the end
			// fst.setRequireFullMatch( ctx.end != null );

			// if we can end anywhere
			if (ctx.end == null) {
				State newFinalState = new State(true);
				newFinalState.addTransition(
						new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.EPSILON, newFinalState, dict)
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
			return FstOperations.repeatMinMax(visit(ctx.repeatexp()), min, max);
		}


		@Override
		public Fst visitRepeatExactlyExpression(RepeatExactlyExpressionContext ctx) {
			int n = Integer.parseInt(ctx.INT().getText());
			return FstOperations.repeatExactly(visit(ctx.repeatexp()), n);
		}
		
		@Override
		public Fst visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
			int max = Integer.parseInt(ctx.INT().getText());
			return FstOperations.repeatMinMax(visit(ctx.repeatexp()), 0, max);
		}

		
		@Override
		public Fst visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT().getText());
			return FstOperations.repeatMin(visit(ctx.repeatexp()), min);
		}

		
		@Override
		public Fst visitSimpleExpression(SimpleExpressionContext ctx) {
			return visit(ctx.simpleexp());
		}

		
		@Override
		public Fst visitPlusExpression(PlusExpressionContext ctx) {
			return FstOperations.plus(visit(ctx.repeatexp()));
		}

		
		@Override
		public Fst visitStarExpression(StarExpressionContext ctx) {
			return FstOperations.kleene(visit(ctx.repeatexp()));
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
			if(capture){
				if(generalize) {
					t = new BasicTransition(0, InputLabelType.SELF, 0, OutputLabelType.SELF_ASCENDANTS, new State(true), dict);
				} else {
					t = new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.SELF, new State(true), dict);
				}
			} else {
				t = new BasicTransition(0, InputLabelType.SELF, -1, OutputLabelType.EPSILON, new State(true), dict);
			}
			fst.getInitialState().addTransition(t);
			fst.updateStates();
			return fst;
		}

		
		@Override
		public Fst visitNonWildCard(NonWildCardContext ctx) {
			boolean generalize = false;
			boolean force = false;
			String word = ctx.item().getText();
			if (word.startsWith("'") && word.endsWith("'") || word.startsWith("\"") && word.endsWith("\"")) {
				// strip the quotes
				word = word.substring(1, word.length()-1);
			}

			Item item = dict.getItemBySid(word);
			if (item == null) {
				throw new RuntimeException("unknown item " + word + " at " + ctx.item().getStart().getLine()
						+ ":" + ctx.item().getStart().getCharPositionInLine());
			}
			int inputLabel = item.fid;
			int opCount = ctx.getChildCount();
			if (opCount == 2) {
				if (ctx.getChild(1).getText().equals("=")) {
					force = true;
				} else {
					generalize = true;
				}
			} else if (opCount == 3) {
				force = true;
				generalize = true;
			}
			
			Fst fst = new Fst();
			InputLabelType inputLabelType;
			int outputLabel;
			OutputLabelType outputLabelType;
			
			if (capture) {
				if (force && generalize) { // case: A=^
					inputLabelType = InputLabelType.SELF_DESCENDANTS;
					outputLabel = inputLabel;
					outputLabelType = OutputLabelType.CONSTANT;
				} else if (force & !generalize) { // case A=
					inputLabelType = InputLabelType.SELF;
					outputLabel = inputLabel;
					outputLabelType = OutputLabelType.CONSTANT;
				} else if (!force && generalize) { // case A^
					inputLabelType = InputLabelType.SELF_DESCENDANTS;
					outputLabel = inputLabel;
					outputLabelType = OutputLabelType.SELF_ASCENDANTS;
				} else { // case A
					inputLabelType = InputLabelType.SELF_DESCENDANTS;
					outputLabel = -1;
					outputLabelType = OutputLabelType.SELF;
				}
			} else {
				assert !generalize;
				outputLabel = -1;
				outputLabelType = OutputLabelType.EPSILON;
				if (force) {
					inputLabelType = InputLabelType.SELF;
				} else {
					inputLabelType = InputLabelType.SELF_DESCENDANTS;
				}
			}
			
			Transition t = new BasicTransition(inputLabel, inputLabelType, outputLabel, outputLabelType, new State(true), dict);
			fst.getInitialState().addTransition(t);
			fst.updateStates();
			return fst;
		}
		
		

	
	}
}

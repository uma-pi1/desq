package de.uni_mannheim.desq.patex;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import de.uni_mannheim.desq.dictionary.Dictionary;
import de.uni_mannheim.desq.fst.BasicTransition;
import de.uni_mannheim.desq.fst.BasicTransition.InputLabelType;
import de.uni_mannheim.desq.fst.BasicTransition.OutputLabelType;
import de.uni_mannheim.desq.fst.Fst;
import de.uni_mannheim.desq.fst.FstOperations;
import de.uni_mannheim.desq.fst.State;
import de.uni_mannheim.desq.fst.Transition;
import de.uni_mannheim.desq.patex.PatExParser.CaptureContext;
import de.uni_mannheim.desq.patex.PatExParser.ConcatContext;
import de.uni_mannheim.desq.patex.PatExParser.ConcatExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.ItemContext;
import de.uni_mannheim.desq.patex.PatExParser.ItemExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.OptionalExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.ParensContext;
import de.uni_mannheim.desq.patex.PatExParser.PlusExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.RepeatExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.RepeatMaxExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.RepeatMinExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.RepeatMinMaxExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.SimpleExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.StarExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.UnionContext;
import de.uni_mannheim.desq.patex.PatExParser.UnionExpressionContext;
import de.uni_mannheim.desq.patex.PatExParser.WildCardContext;





public class PatEx {
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
			return visit(ctx.unionexp());
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
			int min = Integer.parseInt(ctx.WORD(0).getText());
			int max = Integer.parseInt(ctx.WORD(1).getText());
			return FstOperations.repeatMinMax(visit(ctx.repeatexp()), min, max);
		}

		
		@Override
		public Fst visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
			int max = Integer.parseInt(ctx.WORD().getText());
			return FstOperations.repeat(visit(ctx.repeatexp()), max);
		}

		
		@Override
		public Fst visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
			int min = Integer.parseInt(ctx.WORD().getText());
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
		public Fst visitItem(ItemContext ctx) {
			boolean generalize = false;
			boolean force = false;
			String word = ctx.WORD().getText();
			
			int inputLabel = dict.getItemBySid(word).fid;
			
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

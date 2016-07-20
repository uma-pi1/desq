package patex;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import fst.BasicFst;
import fst.Fst;
import fst.FstOperations;
import patex.PatExOldParser.CaptureContext;
import patex.PatExOldParser.ConcatContext;
import patex.PatExOldParser.ConcatExpressionContext;
import patex.PatExOldParser.ItemContext;
import patex.PatExOldParser.ItemExpressionContext;
import patex.PatExOldParser.OptionalExpressionContext;
import patex.PatExOldParser.ParensContext;
import patex.PatExOldParser.PlusExpressionContext;
import patex.PatExOldParser.RepeatExpressionContext;
import patex.PatExOldParser.RepeatMaxExpressionContext;
import patex.PatExOldParser.RepeatMinExpressionContext;
import patex.PatExOldParser.RepeatMinMaxExpressionContext;
import patex.PatExOldParser.SimpleExpressionContext;
import patex.PatExOldParser.StarExpressionContext;
import patex.PatExOldParser.UnionContext;
import patex.PatExOldParser.UnionExpressionContext;
import patex.PatExOldParser.WildCardContext;
import utils.Dictionary;


/**
 * PatExOld.java
 * @author Kaustubh Beedkar {kbeedkar@uni-mannheim.de}
 */
public class PatExOld {

	String expression;
	Dictionary dict = Dictionary.getInstance();

	public PatExOld(String ex) {
		this.expression = ex;
	}

	public Fst translateToFst() {
		ANTLRInputStream input = new ANTLRInputStream(expression);

		// Lexer
		PatExOldLexer lexer = new PatExOldLexer(input);

		// Tokens
		CommonTokenStream tokens = new CommonTokenStream(lexer);

		// Parser
		PatExOldParser parser = new PatExOldParser(tokens);

		// Parse tree
		ParseTree tree = parser.patex();

		// Visitor for parse tree
		Visitor visitor = new Visitor();

		// Create FST from the syntax tree
		Fst fst = visitor.visit(tree);

		return fst;
	}

	public class Visitor extends PatExOldBaseVisitor<Fst> {
		
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
			return FstOperations.repeatMax(visit(ctx.repeatexp()), max);
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
			return BasicFst.translateWildCard(generalize, capture);
		}

		
		@Override
		public Fst visitItem(ItemContext ctx) {
			boolean generalize = false;
			boolean force = false;
			String word = ctx.WORD().getText();
			int label = dict.getItemId(word);
			
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
			return BasicFst.translateItemExpression(label, force, generalize, capture);
		}
		
		

	
	}
}

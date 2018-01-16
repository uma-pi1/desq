package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.patex.PatExParser.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.log4j.Logger;

import java.util.*;


public final class PatExToSequentialPatEx {
	private static final Logger logger = Logger.getLogger(PatExToSequentialPatEx.class);
	private String expression;

	public PatExToSequentialPatEx(String expression) {
		this.expression = expression;
	}

	public String translate() {
		CharStream input = CharStreams.fromString(expression);
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
		// Create sequential PatEx from the syntax tree
		return visitor.visit(tree);
	}

	public class Visitor extends PatExBaseVisitor<String> {

		private boolean capture = false;
		private int unorderedConcatId = -1; //unordered: currently active concat ID (handover in concatExpression)
		private int maxConcatId = -1; //unordered: watermark for easy generation of new concat id
		private ArrayList<HashMap<String,int[]>> unorderedConcatElements = new ArrayList<>(); //unordered: elements of concat with frequencies


		@Override
		public String visitUnorderedExpression(UnorderedExpressionContext ctx) {
			//only called for format concatexp & unorederedexp -> collect unordered items
			if (unorderedConcatId < 0) { //new concatenation
				unorderedConcatId = ++maxConcatId;
				unorderedConcatElements.add(unorderedConcatId, new HashMap<>());
			}
			//remember concatId locally (will be removed by visiting childs)
			int localConcatId = unorderedConcatId;
			//add current fst to backlog
			// (might be added by child already with repeat spec.)
			unorderedConcatElements.get(localConcatId).putIfAbsent(
					visit(ctx.repeatexp()), null
			);
			//handover Id to next recursive step
			unorderedConcatId = localConcatId;
			return visit(ctx.unorderedexp());
		}

		@Override
		public String visitRepeat(RepeatContext ctx) {
			//only as last element of union expression (end of recursion, if there was one)
			if(unorderedConcatId > -1) {
				//remember concat processing id locally, add repeatexp to backlog and permute all
				int localConcatId = unorderedConcatId;
				unorderedConcatElements.get(localConcatId).putIfAbsent(
						visit(ctx.repeatexp()),null
				);
				String permuted = handlePermute(unorderedConcatElements.get(localConcatId));
				unorderedConcatElements.get(localConcatId).clear();
				//return union of Fst permutations (results of concatexp)
				return permuted;
			}else {
				return visit(ctx.repeatexp());
			}
		}


		@Override
		public String visitOptionalExpression(OptionalExpressionContext ctx) {
			if(unorderedConcatId > -1) unorderedConcatId = -1;
			return optional(visit(ctx.repeatexp()));
		}

		
		@Override
		public String visitRepeatMinMaxExpression(RepeatMinMaxExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT(0).getText());
			int max = Integer.parseInt(ctx.INT(1).getText());
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(min,max,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Return original expression but skip unordered repeat operator if present
				return visit(ctx.repeatexp())
						+ reconstructChildren((isUnordered) ? 2 : 1,ctx.children);
			}
		}


		@Override
		public String visitRepeatExactlyExpression(RepeatExactlyExpressionContext ctx) {
			int n = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(n,n,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Return original expression but skip unordered repeat operator if present
				return visit(ctx.repeatexp())
						+ reconstructChildren((isUnordered) ? 2 : 1,ctx.children);
			}
		}
		
		@Override
		public String visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
			int max = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(0,max,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Return original expression but skip unordered repeat operator if present
				return visit(ctx.repeatexp())
						+ reconstructChildren((isUnordered) ? 2 : 1,ctx.children);
			}
		}

		
		@Override
		public String visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
			int min = Integer.parseInt(ctx.INT().getText());
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(min,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Return original expression but skip unordered repeat operator if present
				return visit(ctx.repeatexp())
						+ reconstructChildren((isUnordered) ? 2 : 1,ctx.children);
			}
		}

		
		@Override
		public String visitSimpleExpression(SimpleExpressionContext ctx) {
			if (unorderedConcatId > -1) unorderedConcatId = -1; //stop inheritance
			return visit(ctx.simpleexp());
		}

		
		@Override
		public String visitPlusExpression(PlusExpressionContext ctx) {
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(1,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Return original expression but skip unordered repeat operator if present
				return visit(ctx.repeatexp())
						+ reconstructChildren((isUnordered) ? 2 : 1,ctx.children);
			}
		}

		
		@Override
		public String visitStarExpression(StarExpressionContext ctx) {
			//Check if unordered marker exists
			boolean isUnordered =  ctx.SET() != null;
			if(unorderedConcatId > -1 && isUnordered){ //direct child of unordered concatenation
				return handleUnorderedRepeat(0,0,ctx.repeatexp());
			}else {
				if(isUnordered) addUnorderedWarning();
				//Standard behavior:
				return kleene(visit(ctx.repeatexp()));
			}
		}

		@Override
		public String visitItemExpression(ItemExpressionContext ctx) {
			//Ensure that capture parentheses are added on item level as well as uncaptured adjacent gaps
			//String item = (capture) ? "(" + visit(ctx.itemexp()) + ")" : visit(ctx.itemexp());
			//return "[" + item + ".*]";
			return (capture) ? "(" + visit(ctx.itemexp()) + ")" : visit(ctx.itemexp());
		}
		

		@Override
		public String visitCapture(CaptureContext ctx) {
			//add parentheses only on itemlevel
			capture = true;
			String patEx = visit(ctx.unionexp());
			capture = false;
			return patEx;
		}
		

		private String handleUnorderedRepeat(Integer n, Integer m, RepeatexpContext visit){
			int localConcatId = unorderedConcatId; // remember id locally
			unorderedConcatId = -1; //stop inheritance
			String expr = visit(visit);
			//add frequencies (handled in permutation)
			int[] freq = {(n != null) ? n : 0, (m != null) ? m : 0};
			//Adding expression + repeat specification to backlog
			unorderedConcatElements.get(localConcatId).put(expr,freq);
			return expr;
		}

		private void addUnorderedWarning(){
			logger.warn("Unordered frequency !{n,m} used outside of unordered concatenation (&) -> Treated like sequential '{n,m}'." +
					"Pattern expression: " + expression);
		}

		// -- Default Rule Node and TerminalNode handling (copied from PatExToPatEx)
		@Override
		public String visitChildren(RuleNode node) { //default behavior for visit (if none is implemented)
			//String result = "";
			StringBuilder builder = new StringBuilder();
			int n = node.getChildCount();
			for(int i = 0; i < n; ++i) {
				ParseTree c = node.getChild(i);
				String childResult = c.accept(this);
				builder.append(childResult);
			}
			return builder.toString();
		}

		@Override //return the text of leaf nodes
		public String visitTerminal(TerminalNode node) {
			return node.getText();
		}

		/** Returns an FST that unions all FST permutations */
		private String handlePermute(HashMap<String,int[]> inputExpresssions) {
			ArrayList<String> expressions = new ArrayList<>();
			int connectorSize = 0;
			//handle frequencies
			String connector = null;
			for (Map.Entry<String,int[]> entry: inputExpresssions.entrySet()){
				if(entry.getValue() != null){
					//min occurrences
					int min = entry.getValue()[0];
					int max = entry.getValue()[1];
					if (min > 0){
						expressions.addAll(addExactly(entry.getKey(),min));
					}
					if(max == 0) {
						//no max -> find all occurrences (kleene *) in all combinations -> use as connector
						connector = (connector != null)
								? concatenate(connector, kleene(entry.getKey()))
								: kleene(entry.getKey());
						connectorSize++;

					}else if (max > min){
						//min and max provided
						int dif = max - min;
						//Difference between min and max represented with optionals
						expressions.addAll(addExactly(optional(entry.getKey()), dif));
					}
				}else{
					//No frequencies -> add just once
					expressions.add(entry.getKey());
				}
			}
			//Ensure that connector is optional and can repeat itself -> kleene *
			if(connector != null && connectorSize > 1){
				connector = kleene("[" + connector + "]");
			}

			//start recursion
			String permuted = (expressions.size() > 0) ? permute(null, expressions, connector) : null;

			//add connector at beginning and end as well (if defined)
			if (connector != null) {
				if(permuted != null) {
					permuted = concatenate(concatenate(connector, permuted), connector);
				}else{
					//If nothing to permute (only connector left) -> return connector
					permuted = connector;
				}
			}
			//Encapsulate in [] because of union precedence
			return "[" + permuted + "]";
		}

		private ArrayList<String> addExactly(String expr, int n){
			ArrayList<String> exprList = new ArrayList<>();
			for (int i = 0; i < n; ++i) {
				exprList.add(expr);
			}
			return exprList;
		}

		/** Recursive method to permute all elements - each recursion: define prefix + remaining elements */
		private String permute(String prefixExpr, List<String> expressions, String connector) {
			HashSet<String> usedExpr = new HashSet<>();

			assert expressions.size() > 0;
			if(expressions.size() > 1){
				//more than one item -> permute (recursion step)
				String unionExpr = null;
				for (String expr: expressions){
					if(!usedExpr.contains(expr)) {
						usedExpr.add(expr);
						//add optional connector (eg "[A.*]*.*")to Fst (A.*B.* -> A.*[A.*]*B.*)
						String addedExpr= (connector != null) ? concatenate(expr, connector) : expr;
						//copy remaining expressions into list
						List<String> partExpr = new ArrayList<>(expressions);
						partExpr.remove(expr);

						//handle new prefix
						String newPrefixExpr = (prefixExpr != null)
								//Within recursion: concat items (prefix + new first item)
								? concatenate(prefixExpr,addedExpr)
								//Or first recursion step (no existing prefix yet)
								: addedExpr;

						//recursions combined by union
						unionExpr = (unionExpr != null)
								//union further recursion paths
								? union(unionExpr, permute(newPrefixExpr, partExpr, connector))
								//First loop iteration(first element of union)
								: permute(newPrefixExpr, partExpr, connector);
					}
				}
				return unionExpr;
			}else{
				//end recursion (last concatenation)
				return (prefixExpr != null)
						? concatenate(prefixExpr, expressions.get(0))
						: expressions.get(0); //only one item, no permutation
			}
		}

		private String concatenate(String a, String b){
			return a + " " + b;
		}

		private String union(String a, String b){
			return a + "|" + b;
		}

		private String kleene(String a){
			return a + "*";
		}

		private String optional(String a){
			return a + "?";
		}

		private String reconstructChildren(int startIdx, List<ParseTree> children){
			StringBuilder builder = new StringBuilder();
			int n = children.size();
			for(int i = startIdx; i < n; ++i) {
				builder.append(children.get(i).getText());
			}
			return builder.toString();
		}
	}

}

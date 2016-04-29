// Generated from PatEx.g4 by ANTLR 4.5

    package patex;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PatExParser}.
 */
public interface PatExListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExParser#patex}.
	 * @param ctx the parse tree
	 */
	void enterUnion(PatExParser.UnionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExParser#patex}.
	 * @param ctx the parse tree
	 */
	void exitUnion(PatExParser.UnionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void enterUnionExpression(PatExParser.UnionExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void exitUnionExpression(PatExParser.UnionExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void enterConcat(PatExParser.ConcatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void exitConcat(PatExParser.ConcatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void enterConcatExpression(PatExParser.ConcatExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void exitConcatExpression(PatExParser.ConcatExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatExpression(PatExParser.RepeatExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatExpression(PatExParser.RepeatExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterOptionalExpression(PatExParser.OptionalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitOptionalExpression(PatExParser.OptionalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMinMaxExpression(PatExParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMinMaxExpression(PatExParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMaxExpression(PatExParser.RepeatMaxExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMaxExpression(PatExParser.RepeatMaxExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMinExpression(PatExParser.RepeatMinExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMinExpression(PatExParser.RepeatMinExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterSimpleExpression(PatExParser.SimpleExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitSimpleExpression(PatExParser.SimpleExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterPlusExpression(PatExParser.PlusExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitPlusExpression(PatExParser.PlusExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterStarExpression(PatExParser.StarExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitStarExpression(PatExParser.StarExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterItemExpression(PatExParser.ItemExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitItemExpression(PatExParser.ItemExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterParens(PatExParser.ParensContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitParens(PatExParser.ParensContext ctx);
	/**
	 * Enter a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterCapture(PatExParser.CaptureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitCapture(PatExParser.CaptureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void enterWildCard(PatExParser.WildCardContext ctx);
	/**
	 * Exit a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void exitWildCard(PatExParser.WildCardContext ctx);
	/**
	 * Enter a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void enterItem(PatExParser.ItemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void exitItem(PatExParser.ItemContext ctx);
}
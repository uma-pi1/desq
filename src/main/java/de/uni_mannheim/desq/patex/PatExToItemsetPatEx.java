package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import de.uni_mannheim.desq.patex.PatExParser.*;

import java.util.List;

/** Replaces the ordered versions of frequency and concatenation with unordered equivalent */
public class PatExToItemsetPatEx {

    private BasicDictionary dict;
    private String patternExpression;
    private static final String unorderedConcat = "&";
    private static final String unorderedMarker= "!";


    /** For potential optimizations based on FIDs, the dictionary is needed  {@link Dictionary}. */
    public PatExToItemsetPatEx(BasicDictionary dict, String patternExpression) {
        this.dict = dict;
        this.patternExpression = patternExpression;
    }

    public String translate() {
        CharStream input = CharStreams.fromString(patternExpression);
        PatExLexer lexer = new PatExLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PatExParser parser = new PatExParser(tokens);
        ParseTree tree = parser.patex();
        PatExToItemsetPatEx.Visitor visitor = new PatExToItemsetPatEx.Visitor();
        return visitor.visit(tree);
    }

    class Visitor extends PatExBaseVisitor<String> {
        @Override
        public String visitChildren(RuleNode node) {
            String result = "";
            int n = node.getChildCount();
            for(int i = 0; i < n; ++i) {
                ParseTree c = node.getChild(i);
                String childResult = c.accept(this);
                result += childResult;
            }
            return result;
        }

        @Override
        public String visitConcatExpression(ConcatExpressionContext ctx) {
            //E1 E2 -> E1&E2
            return  visit(ctx.unorderedexp()) + unorderedConcat + visit(ctx.concatexp());
        }

        @Override
        public String visitRepeatMinMaxExpression(RepeatMinMaxExpressionContext ctx) {
            // E{n,m] -> E!{n,m}
            return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitRepeatExactlyExpression(RepeatExactlyExpressionContext ctx) {
            return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitRepeatMaxExpression(RepeatMaxExpressionContext ctx) {
            return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitRepeatMinExpression(RepeatMinExpressionContext ctx) {
            return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitPlusExpression(PlusExpressionContext ctx) {
           return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitStarExpression(StarExpressionContext ctx) {
            return addUnorderedMarker(visit(ctx.repeatexp()),ctx.children);
        }

        @Override
        public String visitTerminal(TerminalNode node) {
            return node.getText();
        }

        private String addUnorderedMarker(String repeatexp, List<ParseTree> children){
            if(children.get(1).getText().equals(unorderedMarker)){
                //already marked as unordered
                return repeatexp + reconstructChildren(1, children);
            }else {
                //convert to unordered
                return repeatexp + unorderedMarker + reconstructChildren(1, children);
            }
        }

        private String reconstructChildren(int startIdx, List<ParseTree> children){
            String result = "";
            int n = children.size();
            for(int i = startIdx; i < n; ++i) {
                result += children.get(i).getText();
            }
            return result;

        }

    }
}

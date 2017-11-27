package de.uni_mannheim.desq.patex;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import de.uni_mannheim.desq.patex.PatExParser.*;

import java.util.List;

/** Replaces the ordered versions of frequency and concatenation with unordered equivalent */
public class PatExToItemsetPatEx implements PatExTranslator<String>{
    private String patternExpression;
    private String itemsetSeparatorSid;


    public PatExToItemsetPatEx(String patternExpression){
        this.patternExpression = patternExpression;
    }

    public PatExToItemsetPatEx(String patternExpression, String itemsetSeparatorSid) {
        this(patternExpression);
        this.itemsetSeparatorSid = itemsetSeparatorSid;
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
        private Boolean capture = false;
        private static final String unorderedConcat = "&";
        private static final String unorderedMarker = "!";
        private static final String negate = "-";

        // -- Handle Concatenations
        @Override
        public String visitConcatExpression(ConcatExpressionContext ctx) {
            //E1 E2 -> E1&E2 - but keep sequence if sequence of itemsets
            return  visit(ctx.unorderedexp())
                    + ((itemsetSeparatorSid == null) ? unorderedConcat : " ")
                    + visit(ctx.concatexp());
        }

        // -- Handle Repeats
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

        // -- Handle capture and gaps

        @Override
        public String visitCapture(CaptureContext ctx) {
            //add parentheses only on itemlevel
            capture = true;
            String patEx = visit(ctx.unionexp());
            capture = false;
            return patEx;
        }

        @Override
        public String visitItemExpression(ItemExpressionContext ctx) {
            //Ensure that capture parentheses are added on item level
            String item = (capture) ? "(" + visit(ctx.itemexp()) + ")" : visit(ctx.itemexp());
            //Handle uncaptured adjacent gaps (with and without item separator)
            item += (itemsetSeparatorSid == null) ? ".*" : negate + itemsetSeparatorSid + "*";
            return "[" + item + "]";
        }

        // -- Helper methods
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
            StringBuilder builder = new StringBuilder();
            int n = children.size();
            for(int i = startIdx; i < n; ++i) {
                builder.append(children.get(i).getText());
            }
            return builder.toString();
        }

        // -- Default Rule Node and TerminalNode handling (copied from PatExToPatEx)
        @Override
        public String visitChildren(RuleNode node) { //default behavior for visit (if none is implemented)
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

    }
}

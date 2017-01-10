package de.uni_mannheim.desq.patex;

import de.uni_mannheim.desq.dictionary.BasicDictionary;
import de.uni_mannheim.desq.dictionary.Dictionary;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

/** Replaces the item references in a pattern expression by the given type of ids. */
public class PatExToPatEx {
    public enum Type { SID, FID, GID };

    BasicDictionary dict;
    String patternExpression;
    Type type;

    /** If the pattern expression contains string item identifiers or type==sid, the dict needs to be an instance
     * of {@link Dictionary}. */
    public PatExToPatEx(BasicDictionary dict, String patternExpression, Type type) {
        this.dict = dict;
        this.patternExpression = patternExpression;
        this.type = type;
    }

    public String translate() {
        ANTLRInputStream input = new ANTLRInputStream(patternExpression);
        PatExLexer lexer = new PatExLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PatExParser parser = new PatExParser(tokens);
        ParseTree tree = parser.patex();
        PatExToPatEx.Visitor visitor = new PatExToPatEx.Visitor();
        String newPatternExpression = visitor.visit(tree);
        return newPatternExpression;
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
        public String visitItem(PatExParser.ItemContext ctx) {
            switch (type) {
                case SID:
                    return "'" + PatExUtils.asSid(dict, ctx) +  "'";
                case FID:
                    return "##" + PatExUtils.asFid(dict, ctx);
                case GID:
                    return "#" + PatExUtils.asGid(dict, ctx);
            }
            throw new IllegalStateException();
        }

        @Override
        public String visitTerminal(TerminalNode node) {
            return node.getText();
        }
    }
}

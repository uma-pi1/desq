package de.uni_mannheim.desq.mining;

public class IntervalNode implements Comparable<IntervalNode>{
    protected int start;
    protected int end;
    protected long support;

    public IntervalNode(int start, int end, long support){
        this.start = start;
        this.end = end;
        this.support = support;
    }

    /**
     *
     * @param other the other node
     * @return
     * >0 if is "larger" than the other node
     * <0 if is "smaller" than the other node
     * 0 if both are equal
     */
    @Override
    public int compareTo(IntervalNode other) {
        //return (start != other.start) ? start - other.start : end - other.end;
        if(start != other.start)
            return start - other.start;
        else if(end != other.end)
            return end - other.end;
        else
            //if exactly same interval: higher support has precedence
            // (parent has higher support than child)
            return (int) (support - other.support);
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.desq.mining;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 *
 * @author Kai-Arne
 */
public class NewPostingList extends AbstractPostingList{

    private ByteArrayList data;
    
    public NewPostingList(){
        this.data = new ByteArrayList();
        this.noPostings = 0;
    }
    
    @Override
    void addNonNegativeIntIntern(int value) {
        assert value >= 0;
        assert size() > 0;
        value += 1; // we add the integer increased by one to distinguish it from separators

        while (true) {
            final int b = value & 0x7F;
            if (value == b) {
                data.add((byte)b);
                return;
            } else {
                data.add((byte)(b | 0x80));
                value >>>= 7;
            }
        }
    }
    
    @Override
    void clear() {
        data.clear();
        noPostings = 0;
    }

    @Override
    int noBytes() { return data.size(); }

    @Override
    void trim() { data.trim(); }

    @Override
    Object getData() { return this.data; }
    
    @Override
    AbstractIterator iterator() { return new Iterator(this); }
    
    public final class Iterator extends AbstractIterator {
        public Iterator() {
            this.data = null;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given data */
        public Iterator(ByteArrayList data) {
            this.data = data;
            this.offset = 0;
        }

        /** Creates an iterator backed by the given posting list */
        public Iterator(AbstractPostingList postingList) {
            this.data = (ByteArrayList) postingList.getData();
            this.offset = 0;
        }
        
        @Override
        int nextNonNegativeIntIntern() {
            int result = 0;
            int shift = 0;
            do {
                final int b = data.getByte(offset);
                offset++;
                result += (b & 0x7F) << shift;
                if (b < 0) {
                    shift += 7;
                    assert shift<32;
                } else {
                    break;
                }
            } while (true);

            assert result >= 1;
            return result - 1; // since we stored ints incremented by 1
        }    
    }
}

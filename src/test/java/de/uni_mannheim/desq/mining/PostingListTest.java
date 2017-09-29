package de.uni_mannheim.desq.mining;

import java.util.Arrays;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author Kai-Arne
 */
@RunWith(Parameterized.class)
public class PostingListTest {
    
    public AbstractPostingList postingList;
    public AbstractIterator iterator;
    public int numberOfElements;
    public int[] inputData;
    
    public PostingListTest(AbstractPostingList pL, AbstractIterator it) {
        this.postingList = pL;
        this.iterator = it;
    }
    
    @Parameterized.Parameters
    public static Collection postingLists() {
        return Arrays.asList(new Object[][] {
            { new BitwiseLongPostingList(), new BitwiseLongPostingList.Iterator() },
            { new EliasGammaPostingList(), new EliasGammaPostingList.Iterator() },
            { new IntegerPostingList(), new IntegerPostingList.Iterator()},
            { new NewPostingList(), new NewPostingList.Iterator() },
            { new VarBytePostingList(), new VarBytePostingList.Iterator()},
            { new VarByteLongPostingList(), new VarByteLongPostingList.Iterator()},
            { new VarByteLongAdvancedPostingList(), new VarByteLongAdvancedPostingList.Iterator()},
        });
    }
    
    @Before
    public void setUp(){
        numberOfElements = 10000;
        inputData = new int[numberOfElements];
                 
        for(int i = 0; i < numberOfElements; i++){
            int prop = (int) (Math.random() * 100);
             
            if(prop < 5){
                inputData[i] = 0;
            } else if (prop >= 5 && prop < 10){
                inputData[i] = (int) (Math.random() * 2000000000);
            } else if (prop >= 20){
                inputData[i] = (int) (Math.random() * 4000);
            }
        }
        
        postingList.newPosting();
        
        for(int i = 0; i < numberOfElements; i++){
            if(i % 10 == 0){
                postingList.newPosting();
            }
            postingList.addNonNegativeInt(inputData[i]);
        }
       
        iterator.reset(postingList);
    }
    
    @Test
    public void postingListTest(){
        int count = 0;
        
        do{
            while(iterator.hasNext()){
                assertEquals(iterator.nextNonNegativeInt(), inputData[count]);
                count++;
            }
        } while(iterator.hasNext());
    }
}

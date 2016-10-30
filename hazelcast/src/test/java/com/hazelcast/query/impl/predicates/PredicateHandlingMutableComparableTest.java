package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests Predicate implementations in their handling of
 * popular Comparables which have mutable state that is non-thread safe
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PredicateHandlingMutableComparableTest {

    private Date referenceDate = new Date();
    private Semaphore waitUntilInputMutated = new Semaphore(0);

    @Test
    public void whenInputMutated_thenEqualPredicateRetainsOriginalSemantics() {
        Date comparedDate = new Date(referenceDate.getTime());
        BlockingPredicate equal = new BlockingPredicate(new EqualPredicate("this", referenceDate));
        mutateInputInSeparateThread(referenceDate.getTime() + 100);
        assertTrue(equal.apply(newMockEntry(comparedDate)));
    }

    @Test
    public void whenInputMutated_thenNotEqualPredicateRetainsOriginalSemantics() {
        Date comparedDate = new Date(referenceDate.getTime() + 100);
        BlockingPredicate notEqual = new BlockingPredicate(new NotEqualPredicate("this", referenceDate));
        mutateInputInSeparateThread(referenceDate.getTime() + 100);
        assertTrue(notEqual.apply(newMockEntry(comparedDate)));
    }

    @Test
    public void whenInputMutated_thenBetweenPredicateRetainsOriginalSemantics() {
        long intervalMillis = 100;
        Date lowerBoundDate = referenceDate;
        Date upperBoundDate = new Date(referenceDate.getTime() + intervalMillis);

        Date comparedDate = new Date(referenceDate.getTime() + intervalMillis / 2);
        BlockingPredicate notEqual = new BlockingPredicate(new BetweenPredicate("this", lowerBoundDate, upperBoundDate));
        mutateInputInSeparateThread(upperBoundDate.getTime());
        assertTrue(notEqual.apply(newMockEntry(comparedDate)));
    }

    @Test
    public void whenInputMutated_thenInPredicateRetainsOriginalSemantics() {
        Date comparedDate = new Date(referenceDate.getTime());
        BlockingPredicate notEqual = new BlockingPredicate(new InPredicate("this", referenceDate));
        mutateInputInSeparateThread(referenceDate.getTime() + 100);
        assertTrue(notEqual.apply(newMockEntry(comparedDate)));
    }

    @Test
    public void whenInputMutated_thenGreaterLessPredicateRetainsOriginalSemantics() {
        Date comparedDate = new Date(referenceDate.getTime() + 100);
        BlockingPredicate notEqual = new BlockingPredicate(new GreaterLessPredicate("this", referenceDate, false, false));
        mutateInputInSeparateThread(referenceDate.getTime() + 100);
        assertTrue(notEqual.apply(newMockEntry(comparedDate)));
    }

    private void mutateInputInSeparateThread(final long newValue) {
        new Thread(new Runnable(){
            @Override
            public void run() {
                referenceDate.setTime(newValue);
                waitUntilInputMutated.release();
            }
        }).start();
    }


    private QueryableEntry newMockEntry(Object attributeValue) {
        QueryableEntry mockEntry = mock(QueryableEntry.class);
        when(mockEntry.getAttributeValue(anyString())).thenReturn(attributeValue);
        return mockEntry;
    }

    private class BlockingPredicate extends AbstractPredicate {

        private final Predicate<?, ?> predicate;

        public BlockingPredicate(AbstractPredicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public int getId() {
            return 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean apply(Map.Entry mapEntry) {
            waitUntilInputMutated.acquireUninterruptibly();
            return predicate.apply(mapEntry);
        }

        @Override
        protected boolean applyForSingleAttributeValue(Map.Entry mapEntry, Comparable attributeValue) {
            throw new UnsupportedOperationException();
        }
    }
}

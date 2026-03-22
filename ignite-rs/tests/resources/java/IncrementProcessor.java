package org.apache.ignite.tests;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/**
 * Entry processor that increments the entry value by the first argument (or 1 if no argument).
 * Used by ignite-rs integration tests.
 */
public class IncrementProcessor implements EntryProcessor<Integer, Integer, Integer> {
    @Override
    public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
        int delta = (arguments != null && arguments.length > 0 && arguments[0] instanceof Integer)
            ? (Integer) arguments[0] : 1;
        int current = entry.exists() ? entry.getValue() : 0;
        int newValue = current + delta;
        entry.setValue(newValue);
        return newValue;
    }
}

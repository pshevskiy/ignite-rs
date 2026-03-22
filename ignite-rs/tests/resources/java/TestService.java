package org.apache.ignite.tests;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Simple service with overloaded methods and call context support.
 * Used by ignite-rs integration tests.
 */
public class TestService implements Service {
    @Override
    public void cancel(ServiceContext ctx) {
        // no-op
    }

    @Override
    public void init(ServiceContext ctx) throws Exception {
        // no-op
    }

    @Override
    public void execute(ServiceContext ctx) throws Exception {
        // no-op
    }

    public String echo(String input) {
        return input;
    }

    public int add(int a, int b) {
        return a + b;
    }

    public String concat(String a, String b) {
        return a + b;
    }

    public void throwException() {
        throw new RuntimeException("Test service exception");
    }
}

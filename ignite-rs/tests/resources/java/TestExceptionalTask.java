package org.apache.ignite.tests;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compute task that always throws an exception.
 * Used by ignite-rs integration tests.
 */
public class TestExceptionalTask extends ComputeTaskAdapter<Integer, Integer> {
    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, Integer arg) {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();
        map.put(new ComputeJobAdapter() {
            @Override
            public Object execute() {
                throw new IgniteException("Test exception from compute task");
            }
        }, nodes.get(0));
        return map;
    }

    @Override
    public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }
}

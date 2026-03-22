package org.apache.ignite.tests;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple compute task that returns the argument multiplied by 2.
 * Used by ignite-rs integration tests.
 */
public class TestTask extends ComputeTaskAdapter<Integer, Integer> {
    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, Integer arg) {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();
        map.put(new ComputeJobAdapter() {
            @Override
            public Object execute() {
                return arg != null ? arg * 2 : 0;
            }
        }, nodes.get(0));
        return map;
    }

    @Override
    public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.get(0).getData();
    }
}

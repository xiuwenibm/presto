/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.newSetFromMap;

public class TestHiveEventListenerPlugin
{
    static class TestingHiveEventListenerPlugin
            implements Plugin
    {
        private final Set<QueryId> runningQueries = newSetFromMap(new ConcurrentHashMap<>());
        private final AtomicInteger totalSplits = new AtomicInteger(0);

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new TestingHiveEventListenerFactory(runningQueries, totalSplits));
        }
    }

    private static class TestingHiveEventListenerFactory
            implements EventListenerFactory
    {
        private final Set<QueryId> runningQueries;
        private AtomicInteger totalSplits;

        public TestingHiveEventListenerFactory(Set<QueryId> runningQueries, AtomicInteger totalSplits)
        {
            this.runningQueries = runningQueries;
            this.totalSplits = totalSplits;
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new TestingHiveEventListener(runningQueries, totalSplits);
        }
    }

    static class TestingHiveEventListener
            implements EventListener
    {
        private final Set<QueryId> runningQueries;
        private final AtomicInteger totalSplits;

        public TestingHiveEventListener(Set<QueryId> runningQueries, AtomicInteger totalSplits)
        {
            this.runningQueries = runningQueries;
            this.totalSplits = totalSplits;
        }

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            runningQueries.add(QueryId.valueOf(queryCreatedEvent.getMetadata().getQueryId()));
        }

        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            QueryId queryId = QueryId.valueOf(queryCompletedEvent.getMetadata().getQueryId());
            if (!runningQueries.contains(queryId)) {
                throw new RuntimeException("Missing create event for query " + queryId);
            }
            // todo: add write the json to file
            runningQueries.remove(queryId);
        }

        @Override
        public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
            totalSplits.incrementAndGet();
        }

        public void resetSplits()
        {
            totalSplits.set(0);
        }

        public int getTotalSplits()
        {
            return totalSplits.get();
        }

        public Set<QueryId> getRunningQueries()
        {
            return runningQueries;
        }
    }
}

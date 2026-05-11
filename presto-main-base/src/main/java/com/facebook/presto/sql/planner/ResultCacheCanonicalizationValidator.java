package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;

import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_RESULT_CACHE;

import java.util.HashSet;
import java.util.Set;

public class ResultCacheCanonicalizationValidator
{
    private final Metadata metadata;
    private final Session session;

    public ResultCacheCanonicalizationValidator(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    /**
     * If any of the tables does not support cache, the result cache is not supported
     */
    public boolean isResultCacheCanonicalizationSupported(PlanNode plan)
    {
        Set<ConnectorId> connectorIds = new HashSet<>();
        plan.accept(new ConnectorCollector(), connectorIds);

        return connectorIds.stream()
                .allMatch(connectorId -> metadata.getConnectorCapabilities(session, connectorId)
                        .contains(SUPPORTS_RESULT_CACHE));
    }

    private static class ConnectorCollector
            extends InternalPlanVisitor<Void, Set<ConnectorId>>
    {
        @Override
        public Void visitTableScan(TableScanNode node, Set<ConnectorId> connectorIds)
        {
            connectorIds.add(node.getTable().getConnectorId());
            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Set<ConnectorId> connectorIds)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, connectorIds);
            }
            return null;
        }
    }
}
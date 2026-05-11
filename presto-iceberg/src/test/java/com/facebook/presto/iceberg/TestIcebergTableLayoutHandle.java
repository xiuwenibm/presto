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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.DEFAULT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestIcebergTableLayoutHandle
{
    private static final String SCHEMA = "test_schema";
    private static final String TABLE = "test_table";

    @Test
    public void testIdentifierDiffersOnSnapshotChange()
    {
        // Two layouts that differ only in snapshot id should produce different
        // identifiers, otherwise fragment-result caching would serve stale data
        // after every Iceberg commit.
        Object idAt100 = layoutAtSnapshot(100L)
                .getIdentifier(Optional.empty(), DEFAULT);
        Object idAt101 = layoutAtSnapshot(101L)
                .getIdentifier(Optional.empty(), DEFAULT);

        assertNotEquals(idAt100, idAt101);
        assertNotEquals(idAt100.hashCode(), idAt101.hashCode());
    }

    @Test
    public void testIdentifierStableForSameSnapshot()
    {
        // Two layouts built independently with the same snapshot id and the same
        // (trivial) predicate must produce equal identifiers, otherwise the cache
        // would never hit even when the data is identical.
        Object id1 = layoutAtSnapshot(100L)
                .getIdentifier(Optional.empty(), DEFAULT);
        Object id2 = layoutAtSnapshot(100L)
                .getIdentifier(Optional.empty(), DEFAULT);

        assertEquals(id1, id2);
        assertEquals(id1.hashCode(), id2.hashCode());
    }

    @Test
    public void testIdentifierDiffersOnChangelogEndSnapshot()
    {
        // Same start snapshot, different changelog end snapshot ⇒ different read range,
        // so the identifier must differ.
        Object id1 = layout(Optional.of(50L), Optional.of(100L))
                .getIdentifier(Optional.empty(), DEFAULT);
        Object id2 = layout(Optional.of(50L), Optional.of(101L))
                .getIdentifier(Optional.empty(), DEFAULT);

        assertNotEquals(id1, id2);
    }

    private static IcebergTableLayoutHandle layoutAtSnapshot(long snapshotId)
    {
        return layout(Optional.of(snapshotId), Optional.empty());
    }

    private static IcebergTableLayoutHandle layout(Optional<Long> snapshotId, Optional<Long> changelogEndSnapshot)
    {
        IcebergTableName icebergTableName = new IcebergTableName(
                TABLE,
                DATA,
                snapshotId,
                Optional.empty(),
                changelogEndSnapshot);

        IcebergTableHandle tableHandle = new IcebergTableHandle(
                SCHEMA,
                icebergTableName,
                snapshotId.isPresent(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        TupleDomain<Subfield> domainPredicate = TupleDomain.all();
        RowExpression remainingPredicate = new ConstantExpression(true, BOOLEAN);
        TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.all();

        return new IcebergTableLayoutHandle(
                ImmutableList.<IcebergColumnHandle>of(),
                ImmutableList.of(),
                domainPredicate,
                remainingPredicate,
                ImmutableMap.of(),
                Optional.empty(),
                false,
                partitionColumnPredicate,
                tableHandle);
    }
}

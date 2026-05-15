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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Deterministic fingerprint over the subset of a {@link Session}'s overridden session-property
 * values that can affect query results.
 *
 * <p>Only properties whose {@link PropertyMetadata#isResultAffecting()} flag is {@code true} —
 * i.e. ones the system or a connector has explicitly opted in via
 * {@link SessionPropertyManager#getResultAffectingProperties()} — contribute. Properties left at
 * their default are omitted so cluster-wide default tweaks don't churn cache keys, and so a
 * session with no overrides yields a stable empty-fingerprint string.
 *
 * <p>The fingerprint is the SHA-256 of {@code key=value\n} pairs (keys sorted; key is the
 * qualified property name returned by {@code getResultAffectingProperties()}). Empty input
 * still produces a stable sha256 string rather than {@code null} so equals comparisons of
 * canonical plan fragments don't have to special-case absence.
 */
public final class SessionPropertiesFingerprint
{
    private SessionPropertiesFingerprint() {}

    public static String compute(Session session, SessionPropertyManager sessionPropertyManager)
    {
        Map<String, PropertyMetadata<?>> resultAffecting = sessionPropertyManager.getResultAffectingProperties();
        if (resultAffecting.isEmpty()) {
            return emptyFingerprint();
        }

        Map<String, String> systemOverrides = session.getSystemProperties();
        Map<ConnectorId, Map<String, String>> connectorOverrides = session.getConnectorProperties();

        ImmutableSortedMap.Builder<String, String> selected = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<String, PropertyMetadata<?>> entry : resultAffecting.entrySet()) {
            String qualifiedName = entry.getKey();
            PropertyMetadata<?> metadata = entry.getValue();
            String defaultValue = metadata.getDefaultValue() == null ? "" : metadata.getDefaultValue().toString();

            String overrideValue = resolveOverride(qualifiedName, metadata.getName(), systemOverrides, connectorOverrides);
            if (overrideValue == null || Objects.equals(overrideValue, defaultValue)) {
                // Skip properties at their default — admin-wide default changes shouldn't
                // invalidate per-query cache entries, and a session with no overrides should
                // produce the stable empty fingerprint.
                continue;
            }
            selected.put(qualifiedName, overrideValue);
        }

        SortedMap<String, String> entries = selected.build();
        if (entries.isEmpty()) {
            return emptyFingerprint();
        }

        StringBuilder serialized = new StringBuilder();
        for (Map.Entry<String, String> e : entries.entrySet()) {
            serialized.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        return sha256().hashString(serialized.toString(), UTF_8).toString();
    }

    /**
     * For a system property {@code foo}, the qualified name is the same as the metadata name
     * and the overridden value (if any) lives in {@link Session#getSystemProperties()}.
     * For a connector property {@code <connectorId>.bar}, the override lives in
     * {@link Session#getConnectorProperties(ConnectorId)} keyed by the bare property name.
     */
    private static String resolveOverride(
            String qualifiedName,
            String bareName,
            Map<String, String> systemOverrides,
            Map<ConnectorId, Map<String, String>> connectorOverrides)
    {
        if (qualifiedName.equals(bareName)) {
            return systemOverrides.get(bareName);
        }
        int dot = qualifiedName.indexOf('.');
        if (dot < 0) {
            return null;
        }
        ConnectorId connectorId = new ConnectorId(qualifiedName.substring(0, dot));
        Map<String, String> perConnector = connectorOverrides.get(connectorId);
        return perConnector == null ? null : perConnector.get(bareName);
    }

    private static String emptyFingerprint()
    {
        return sha256().hashString("", UTF_8).toString();
    }
}

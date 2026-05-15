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
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestSessionPropertiesFingerprint
{
    private static final String RESULT_AFFECTING_BOOL = "test_legacy_timestamp";
    private static final String RESULT_AFFECTING_STRING = "test_decimal_literal_mode";
    private static final String OPERATIONAL_BOOL = "test_query_max_execution_time_enabled";

    private static List<PropertyMetadata<?>> properties()
    {
        return ImmutableList.of(
                booleanProperty(RESULT_AFFECTING_BOOL, "stub", false, false).withResultAffecting(true),
                stringProperty(RESULT_AFFECTING_STRING, "stub", "default-mode", false).withResultAffecting(true),
                // Deliberately NOT result-affecting: an operational knob.
                booleanProperty(OPERATIONAL_BOOL, "stub", false, false));
    }

    private static SessionPropertyManager newManager()
    {
        return createTestingSessionPropertyManager(properties());
    }

    @Test
    public void testSessionWithNoOverridesYieldsStableEmptyFingerprint()
    {
        SessionPropertyManager manager = newManager();
        Session session = testSessionBuilder(manager).build();

        String fingerprint = SessionPropertiesFingerprint.compute(session, manager);

        // Two computations against an unchanged session must produce the same string.
        assertEquals(fingerprint, SessionPropertiesFingerprint.compute(session, manager));

        // And the no-override fingerprint must match a freshly built session with the same
        // property set — i.e. the value is determined by the inputs, not by session identity.
        Session another = testSessionBuilder(newManager()).build();
        assertEquals(fingerprint, SessionPropertiesFingerprint.compute(another, manager));
    }

    @Test
    public void testOverridingResultAffectingPropertyChangesFingerprint()
    {
        SessionPropertyManager manager = newManager();
        Session base = testSessionBuilder(manager).build();
        Session withOverride = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_BOOL, "true")
                .build();

        String baseFp = SessionPropertiesFingerprint.compute(base, manager);
        String overrideFp = SessionPropertiesFingerprint.compute(withOverride, manager);

        assertNotEquals(baseFp, overrideFp,
                "expected overriding a result-affecting property to change the fingerprint");
    }

    @Test
    public void testOverridingOperationalPropertyDoesNotChangeFingerprint()
    {
        // Properties not marked result-affecting (the typical "operational" knobs like query
        // priority or execution-time limits) must be excluded from the fingerprint so toggling
        // them doesn't churn the fragment-result cache.
        SessionPropertyManager manager = newManager();
        Session base = testSessionBuilder(manager).build();
        Session withOverride = testSessionBuilder(manager)
                .setSystemProperty(OPERATIONAL_BOOL, "true")
                .build();

        assertEquals(
                SessionPropertiesFingerprint.compute(base, manager),
                SessionPropertiesFingerprint.compute(withOverride, manager));
    }

    @Test
    public void testSettingResultAffectingPropertyToItsDefaultDoesNotChangeFingerprint()
    {
        // Explicit override to the same value as the default should be indistinguishable from
        // no override at all — otherwise admin tweaks to defaults would invalidate caches even
        // when the *effective* value didn't change.
        SessionPropertyManager manager = newManager();
        Session base = testSessionBuilder(manager).build();
        Session settingDefault = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_BOOL, "false") // matches default
                .build();

        assertEquals(
                SessionPropertiesFingerprint.compute(base, manager),
                SessionPropertiesFingerprint.compute(settingDefault, manager));
    }

    @Test
    public void testDifferentOverrideValuesProduceDifferentFingerprints()
    {
        SessionPropertyManager manager = newManager();
        Session a = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_STRING, "value-a")
                .build();
        Session b = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_STRING, "value-b")
                .build();

        assertNotEquals(
                SessionPropertiesFingerprint.compute(a, manager),
                SessionPropertiesFingerprint.compute(b, manager));
    }

    @Test
    public void testFingerprintIsDeterministicForSameInputs()
    {
        // The fingerprint must depend only on the (key, value) set of result-affecting
        // overrides — not on insertion order, not on session-builder side effects.
        SessionPropertyManager manager = newManager();
        Session a = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_BOOL, "true")
                .setSystemProperty(RESULT_AFFECTING_STRING, "x")
                .build();
        Session b = testSessionBuilder(manager)
                .setSystemProperty(RESULT_AFFECTING_STRING, "x")
                .setSystemProperty(RESULT_AFFECTING_BOOL, "true")
                .build();

        assertEquals(
                SessionPropertiesFingerprint.compute(a, manager),
                SessionPropertiesFingerprint.compute(b, manager));
    }

    @Test
    public void testManagerWithNoResultAffectingPropertiesYieldsEmptyFingerprint()
    {
        // Even with a fully default manager, the method must return a stable, non-null string
        // (the sha256 of the empty input). Callers should never have to special-case absence.
        SessionPropertyManager manager = createTestingSessionPropertyManager(ImmutableList.of(
                booleanProperty(OPERATIONAL_BOOL, "stub", false, false)));
        Session session = testSessionBuilder(manager).build();

        String fingerprint = SessionPropertiesFingerprint.compute(session, manager);
        // Same as the empty-input sha256 — see SessionPropertiesFingerprint.emptyFingerprint.
        assertEquals(fingerprint, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }
}

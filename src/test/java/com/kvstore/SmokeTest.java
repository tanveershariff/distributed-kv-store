package com.kvstore;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M0 smoke test — verifies Maven + JUnit 5 wiring compiles and runs.
 * Replaced by real tests starting in M1.
 */
class SmokeTest {

    @Test
    void mavenAndJunit5WiringIsCorrect() {
        // If this executes, Surefire found the JUnit 5 Jupiter engine.
        assertTrue(true, "Maven + JUnit 5 wiring OK");
    }
}

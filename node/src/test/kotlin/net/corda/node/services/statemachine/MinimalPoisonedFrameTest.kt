package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.utilities.getOrThrow
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.StartedMockNode
import net.corda.testing.node.internal.enclosedCordapp
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.Duration
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Minimal reproduction of a Quasar frame-record misread that leaves an ordinary flow stuck in the hospital.
 *
 * A subflow keeps sixteen `long` locals live across `sleep(0)`. Quasar saves them into the fiber's stack array
 * at the subflow's frame and, because `Stack.popMethod` clears only the reference slots, leaves them there when
 * the subflow returns. The LeaveSubFlow transition then enters an instrumented Kotlin lambda
 * (`TopLevelTransition$leaveSubFlowTransition$1.invoke`, instrumented by Quasar's `KotlinClassifier`) from
 * uninstrumented code, so `Stack.nextMethodEntry` reads one of those stale slots as the lambda's own frame
 * record. If the value's top 14 bits are 1, the lambda "resumes" at a call site it never suspended at, with null
 * locals, throws `NullPointerException: … TopLevelTransition.containsTimedFlows … "$this" is null`, and the flow
 * ends in the hospital, never completing.
 */
class MinimalPoisonedFrameTest {

    private lateinit var mockNet: MockNetwork
    private lateinit var node: StartedMockNode

    @Before
    fun setUp() {
        mockNet = MockNetwork(MockNetworkParameters(cordappsForAllNodes = listOf(enclosedCordapp())))
        node = mockNet.createPartyNode()
        mockNet.startNodes()
    }

    @After
    fun tearDown() = mockNet.stopNodes()

    @Test(timeout = 300_000)
    fun `value below 2^50 - flow completes`() {
        val safe = (1L shl 50) - 1                     // top 14 bits = 0
        assertEquals(16 * safe, run(safe))
    }

    @Test(timeout = 300_000)
    fun `value at 2^50 - flow never completes`() {
        val poison = 1L shl 50                         // top 14 bits = 1
        assertFailsWith<TimeoutException> { run(poison) }
    }

    private fun run(value: Long): Long {
        val future = node.startFlow(MinimalParentFlow(value))
        mockNet.runNetwork()
        return future.getOrThrow(Duration.ofSeconds(30))
    }

    class MinimalParentFlow(private val value: Long) : FlowLogic<Long>() {
        @Suspendable
        override fun call(): Long = subFlow(MinimalSubFlow(value))   // LeaveSubFlow for MinimalSubFlow fires in here
    }

    class MinimalSubFlow(private val value: Long) : FlowLogic<Long>() {
        @Suspendable
        override fun call(): Long {
            val p0 = value
            val p1 = value
            val p2 = value
            val p3 = value
            val p4 = value
            val p5 = value
            val p6 = value
            val p7 = value
            val p8 = value
            val p9 = value
            val p10 = value
            val p11 = value
            val p12 = value
            val p13 = value
            val p14 = value
            val p15 = value
            sleep(Duration.ZERO)                        // checkpoint; the sixteen longs are saved into the fiber stack here
            return p0 + p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10 + p11 + p12 + p13 + p14 + p15
        }
    }
}

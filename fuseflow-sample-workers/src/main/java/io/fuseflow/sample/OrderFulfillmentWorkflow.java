package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Workflow;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * A second annotation-defined workflow (Phase 6) in the <b>self-contained</b> style: the
 * {@code @Workflow} class's own {@code @Activity} methods <b>are</b> the workflow's steps.
 * This is the minimal way to define a workflow with its activities — one class, no separate
 * worker file, no duplicated names:
 * <ul>
 *   <li>the activity name is the method name (blank {@code @Activity} value, Temporal-style);</li>
 *   <li>{@code @Activity.id} is the task id in the DAG (defaults to the activity name) and
 *       {@code @Activity.dependsOn} the edges — {@code validate} → {@code charge} + {@code pack}
 *       (parallel) → {@code ship} (fan-in join) → {@code notify};</li>
 *   <li>the same methods are registered as executable activities by the {@code ActivityScanner},
 *       so this bean both defines and implements the workflow;</li>
 * </ul>
 *
 * <p>Compare {@link ImageProcessingWorkflow}, which uses the metadata-only {@code @Step} style
 * (activities declared in separate, individually-gated classes — needed when a fleet instance
 * advertises only a subset of activities, e.g. the Phase 5 io/media split).
 */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-order", havingValue = "true", matchIfMissing = true)
@Workflow(name = "order-fulfillment",
        description = "Order processing: validate, charge + pack in parallel, ship, notify (Phase 6)")
public class OrderFulfillmentWorkflow extends AbstractSampleWorker {

    public OrderFulfillmentWorkflow(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity(id = "validate")
    public Map<String, Object> validateOrder(ActivityContext ctx) {
        return run("validateOrder", ctx);
    }

    @Activity(id = "charge", dependsOn = "validate")
    public Map<String, Object> chargePayment(ActivityContext ctx) {
        return run("chargePayment", ctx);
    }

    @Activity(id = "pack", dependsOn = "validate")
    public Map<String, Object> packItems(ActivityContext ctx) {
        return run("packItems", ctx);
    }

    @Activity(id = "ship", dependsOn = {"charge", "pack"})
    public Map<String, Object> shipOrder(ActivityContext ctx) {
        return run("shipOrder", ctx);
    }

    @Activity(id = "notify", dependsOn = "ship")
    public Map<String, Object> notifyCustomer(ActivityContext ctx) {
        return run("notifyCustomer", ctx);
    }
}

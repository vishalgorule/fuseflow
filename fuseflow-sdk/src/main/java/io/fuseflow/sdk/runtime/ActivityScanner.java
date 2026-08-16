package io.fuseflow.sdk.runtime;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import io.fuseflow.sdk.core.ActivityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Scans all Spring beans for {@code @Activity}-annotated methods and registers them in the
 * {@link ActivityRegistry}. Runs via {@link SmartInitializingSingleton} — after all singleton
 * beans exist (so proxy and late beans are visible) and before the worker's
 * {@code ApplicationRunner} registration.
 */
public class ActivityScanner implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(ActivityScanner.class);

    private final ApplicationContext applicationContext;
    private final ActivityRegistry registry;

    public ActivityScanner(ApplicationContext applicationContext, ActivityRegistry registry) {
        this.applicationContext = applicationContext;
        this.registry = registry;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // All singletons already exist at this point, so no eager initialization is needed
        // (avoids force-initializing @Lazy beans in larger applications).
        for (String beanName : applicationContext.getBeanNamesForType(Object.class, false, false)) {
            Object bean = applicationContext.getBean(beanName);
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            for (Method method : targetClass.getMethods()) {
                Activity annotation = method.getAnnotation(Activity.class);
                if (annotation != null) {
                    // Phase 6: blank value = activity name is the method name (Temporal-style).
                    String name = annotation.value() == null || annotation.value().isBlank()
                            ? method.getName() : annotation.value();
                    registerMethod(bean, method, name);
                }
            }
        }
        if (!registry.names().isEmpty()) {
            log.info("Registered {} worker activity(ies): {}", registry.names().size(), registry.names());
        }
    }

    private void registerMethod(Object bean, Method method, String name) {
        if (method.getParameterCount() != 1
                || !ActivityContext.class.isAssignableFrom(method.getParameterTypes()[0])) {
            throw new IllegalStateException("@Activity method '" + method + "' must take a single "
                    + ActivityContext.class.getSimpleName() + " parameter");
        }
        method.setAccessible(true);
        ActivityHandler handler = context -> {
            try {
                return method.invoke(bean, context);
            } catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof Exception checked) {
                    throw checked;
                }
                throw ex;
            }
        };
        registry.register(name, handler);
    }
}

/**
 * Copyright (c) 2013-2019 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.spring.support;

import org.redisson.api.RDestroyable;
import org.redisson.api.RedissonClient;
import org.redisson.api.annotation.RInject;
import org.redisson.misc.Injector;
import org.redisson.misc.InjectionContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;

import java.lang.annotation.Annotation;

/**
 *
 * @author Rui Gu (https://github.com/jackygurui)
 *
 */
public class RInjectBeanPostProcessor extends InstantiationAwareBeanPostProcessorAdapter
        implements DestructionAwareBeanPostProcessor, PriorityOrdered, BeanFactoryAware {

    private final Class<? extends Annotation> rInject = RInject.class;
    private final int order = Ordered.LOWEST_PRECEDENCE - 1;
    private BeanFactory beanFactory;
    private InjectionContext injectionContext;

    /**
     * Callback that supplies the owning factory to a bean instance.
     * <p>Invoked after the population of normal bean properties
     * but before an initialization callback such as
     * {@link InitializingBean#afterPropertiesSet()} or a custom init-method.
     *
     * @param beanFactory owning BeanFactory (never {@code null}).
     *                    The bean can immediately call methods on the factory.
     * @throws BeansException in case of initialization errors
     * @see BeanInitializationException
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
        this.injectionContext = new SpringContextAwareInjectionContext(beanFactory);
    }

    public Object postProcessAfterInitialization(Object bean, String beanName)
            throws BeansException {
        Injector.inject(bean, injectionContext);
        return bean;
    }

    /**
     * Apply this BeanPostProcessor to the given bean instance before its
     * destruction, e.g. invoking custom destruction callbacks.
     * <p>Like DisposableBean's {@code destroy} and a custom destroy method, this
     * callback will only apply to beans which the container fully manages the
     * lifecycle for. This is usually the case for singletons and scoped beans.
     *
     * @param bean     the bean instance to be destroyed
     * @param beanName the name of the bean
     * @throws BeansException in case of errors
     * @see DisposableBean#destroy()
     * @see AbstractBeanDefinition#setDestroyMethodName(String)
     */
    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) throws BeansException {
        if (bean instanceof RDestroyable) {
            ((RDestroyable) bean).destroy();
        }
        if (bean instanceof RedissonClient) {
            RedissonClient redissonClient = (RedissonClient) bean;
            if (!redissonClient.isShutdown() || !redissonClient.isShuttingDown()) {
                redissonClient.shutdown();
            }
        }
    }

    /**
     * Determine whether the given bean instance requires destruction by this
     * post-processor.
     * <p><b>NOTE:</b> Even as a late addition, this method has been introduced on
     * {@code DestructionAwareBeanPostProcessor} itself instead of on a SmartDABPP
     * subinterface. This allows existing {@code DestructionAwareBeanPostProcessor}
     * implementations to easily provide {@code requiresDestruction} logic while
     * retaining compatibility with Spring <4.3, and it is also an easier onramp to
     * declaring {@code requiresDestruction} as a Java 8 default method in Spring 5.
     * <p>If an implementation of {@code DestructionAwareBeanPostProcessor} does
     * not provide a concrete implementation of this method, Spring's invocation
     * mechanism silently assumes a method returning {@code true} (the effective
     * default before 4.3, and the to-be-default in the Java 8 method in Spring 5).
     *
     * @param bean the bean instance to check
     * @return {@code true} if {@link #postProcessBeforeDestruction} is supposed to
     * be called for this bean instance eventually, or {@code false} if not needed
     */
    public boolean requiresDestruction(Object bean) {
        return Boolean.TRUE;
    }

    @Override
    public int getOrder() {
        return order;
    }
}

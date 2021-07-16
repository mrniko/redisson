/**
 * Copyright (c) 2013-2021 Nikita Koksharov
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
package org.redisson.spring.transaction;

import org.redisson.api.RTransaction;
import org.redisson.api.RedissonClient;
import org.redisson.api.TransactionOptions;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RedissonTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

    private static final long serialVersionUID = -6151310954082124041L;
    
    private RedissonClient redisson;
    
    public RedissonTransactionManager(RedissonClient redisson) {
        this.redisson = redisson;
    }
    
    public RTransaction getCurrentTransaction() {
        RedissonTransactionHolder to = (RedissonTransactionHolder) TransactionSynchronizationManager.getResource(redisson);
        if (to == null) {
            throw new NoTransactionException("No transaction is available for the current thread");
        }
        return to.getTransaction();
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        RedissonTransactionObject transactionObject = new RedissonTransactionObject();
        
        RedissonTransactionHolder holder = (RedissonTransactionHolder) TransactionSynchronizationManager.getResource(redisson);
        if (holder != null) {
            transactionObject.setTransactionHolder(holder);
        }
        return transactionObject;
    }
    
    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        RedissonTransactionObject transactionObject = (RedissonTransactionObject) transaction;
        return transactionObject.getTransactionHolder() != null;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        RedissonTransactionObject tObject = (RedissonTransactionObject) transaction;
        
        if (tObject.getTransactionHolder() == null) {
            int timeout = determineTimeout(definition);
            TransactionOptions options = TransactionOptions.defaults();
            if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
                options.timeout(timeout, TimeUnit.SECONDS);
            }
            
            RTransaction trans = redisson.createTransaction(options);
            RedissonTransactionHolder holder = new RedissonTransactionHolder();
            holder.setTransaction(trans);
            tObject.setTransactionHolder(holder);
            TransactionSynchronizationManager.bindResource(redisson, holder);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        RedissonTransactionObject to = (RedissonTransactionObject) status.getTransaction();
        try {
            to.getTransactionHolder().getTransaction().commit();
        } catch (org.redisson.transaction.TransactionException e) {
            throw new TransactionSystemException("Unable to commit transaction", e);
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        RedissonTransactionObject to = (RedissonTransactionObject) status.getTransaction();
        try {
            to.getTransactionHolder().getTransaction().rollback();
        } catch (org.redisson.transaction.TransactionException e) {
            throw new TransactionSystemException("Unable to rollback transaction", e);
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        RedissonTransactionObject to = (RedissonTransactionObject) status.getTransaction();
        to.setRollbackOnly(true);
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources) throws TransactionException {
        TransactionSynchronizationManager.bindResource(redisson, suspendedResources);
    }

    @Override
    protected Object doSuspend(Object transaction) throws TransactionException {
        RedissonTransactionObject to = (RedissonTransactionObject) transaction;
        to.setTransactionHolder(null);
        return TransactionSynchronizationManager.unbindResource(redisson);
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        TransactionSynchronizationManager.unbindResourceIfPossible(redisson);
        RedissonTransactionObject to = (RedissonTransactionObject) transaction;
        to.getTransactionHolder().setTransaction(null);
    }
    
    @Override
    public Object getResourceFactory() {
        return redisson;
    }

    @Override
    protected Object doSuspend(Object transaction) throws TransactionException {
        RedissonTransactionObject txObject = (RedissonTransactionObject) transaction;
        txObject.setTransactionHolder(null);
        ConnectionHolder conHolder = (ConnectionHolder)
                TransactionSynchronizationManager.unbindResource(this.getResourceFactory());
        return conHolder;
    }

    @Override
    protected void doResume(Object transaction, Object suspendedResources) throws TransactionException {
        ConnectionHolder conHolder = (ConnectionHolder) suspendedResources;
        TransactionSynchronizationManager.bindResource(this.getResourceFactory(), conHolder);
    }

}

/*
 * Copyright 2011, Red Hat, Inc. and individual contributors
 * Copyright 2022-2023, Mobius Software LTD. and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.restcomm.cache.infinispan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.context.Flag;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.restcomm.cache.infinispan.tree.Node;
import org.restcomm.cache.infinispan.tree.TreeCache;
import org.restcomm.cache.infinispan.tree.TreeCacheFactory;
import org.restcomm.cluster.AsyncCacheCallback;
import org.restcomm.cluster.CacheDataExecutorService;
import org.restcomm.cluster.CacheDataExecutorService.CommonCommand;
import org.restcomm.cluster.IDGenerator;
import org.restcomm.cluster.RestcommCluster;
import org.restcomm.cluster.TreeTransactionContextThreadLocal;
import org.restcomm.cluster.TreeTxState;
import org.restcomm.cluster.data.AbstractTreeSegment;
import org.restcomm.cluster.data.ClusteredIDAndStringKey;
import org.restcomm.cluster.data.ClusteredUUIDExternalizer;
import org.restcomm.cluster.data.ExternalizableExternalizer;
import org.restcomm.cluster.data.MultiStringKey;
import org.restcomm.cluster.data.RootTreeSegment;
import org.restcomm.cluster.data.StringAndClusteredIDKey;
import org.restcomm.cluster.data.TreePutIfAbsentResult;
import org.restcomm.cluster.data.TreeSegment;
import org.restcomm.cluster.data.WriteOperation;
import org.restcomm.cluster.serializers.JbossSerializer;
import org.restcomm.cluster.serializers.Serializer;

/**
 * The container's HA and FT data source.
 *
 * @author martins
 * @author András Kőkuti
 * @author yulian.oifa
 */
public class InfinispanCache {

    private static Logger logger = LogManager.getLogger(InfinispanCache.class);

    private AdvancedCache<Object, Object> jbossCache;
    private TreeCache treeCache;
    
    private boolean localMode;
    private boolean isAsync;
    private boolean isTree;
    private String name;
    private Boolean logStats;
    
    private CacheDataExecutorService cacheDataExecutorService;
    private IDGenerator<?> generator;
    
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private DefaultCacheManager jBossCacheContainer;
    private CounterConfiguration counterConfiguration;
    private CounterManager counterManager;
    private ClusteredLockManager lockManager;
    private ConcurrentHashMap<String,AtomicLong> localCounters=new ConcurrentHashMap<String, AtomicLong>();    
    private ConcurrentHashMap<String,Lock> localLocks=new ConcurrentHashMap<String, Lock>();    
    private TransactionManager txMgr;  
    
    public InfinispanCache(String name, DefaultCacheManager jBossCacheContainer, TransactionManager txMgr, ClassLoader classLoader, CacheDataExecutorService cacheDataExecutorService,IDGenerator<?> generator,Boolean isTree,Boolean logStats) {
        this.cacheDataExecutorService = cacheDataExecutorService;
        this.jBossCacheContainer=jBossCacheContainer;
        this.txMgr=txMgr;
        this.generator=generator;
        this.logStats=logStats;
        
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);
				
		if (!jBossCacheContainer.getDefaultCacheConfiguration().clustering().cacheMode().isClustered()) {
            localMode = true;
        }     

		if(jBossCacheContainer.getDefaultCacheConfiguration().clustering().cacheMode()==CacheMode.DIST_ASYNC || jBossCacheContainer.getDefaultCacheConfiguration().clustering().cacheMode()==CacheMode.REPL_ASYNC)
			this.isAsync=true;
		else
			this.isAsync=false;
		
		this.isTree=isTree;
        this.name = name;
        
        counterConfiguration = CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(0).storage(Storage.VOLATILE).build();		
        Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    public static DefaultCacheManager initContainer(String clusterName, TransactionManager txMgr,Serializer serializer,Boolean isAsync, Boolean isReplicated,Boolean isParititioned,Integer copies,Integer aquireTimeout, Long maxIdleMs) {
    	DefaultCacheManager jBossCacheContainer;
    	TransactionManagerLookup txLookup=new TransactionManagerLookup() {				
			@Override
			public TransactionManager getTransactionManager() throws Exception {
				return txMgr;
			}
		};
		
		if(isReplicated) {
			ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
			if(maxIdleMs!=null)
				defaultConfig.expiration().maxIdle(maxIdleMs, TimeUnit.SECONDS);
			
			if(isAsync) {
				if(isParititioned)
					defaultConfig.clustering().cacheMode(CacheMode.DIST_ASYNC).hash().numOwners(copies).transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).locking().isolationLevel(IsolationLevel.READ_COMMITTED).expiration().jmxStatistics().disable();
				else
					defaultConfig.clustering().cacheMode(CacheMode.REPL_ASYNC).transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable().build();
			} 
			else if(txMgr==null) {
				if(isParititioned)				
					defaultConfig.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(copies).transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable();
				else
					defaultConfig.clustering().cacheMode(CacheMode.REPL_SYNC).transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable();
			}
			else if(isParititioned)				
				defaultConfig.invocationBatching().enable().clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(copies).transaction().transactionManagerLookup(txLookup).lockingMode(LockingMode.PESSIMISTIC).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable();
			else
				defaultConfig.invocationBatching().enable().clustering().cacheMode(CacheMode.REPL_SYNC).transaction().transactionManagerLookup(txLookup).lockingMode(LockingMode.PESSIMISTIC).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable();
						
			
			GlobalConfigurationBuilder gcBuilder=new GlobalConfigurationBuilder();	
			CounterManagerConfigurationBuilder counterBuilder = gcBuilder.addModule(CounterManagerConfigurationBuilder.class);
			counterBuilder.numOwner(copies).reliability(Reliability.AVAILABLE);
		    gcBuilder.defaultCacheName("slee-default").transport().clusterName(clusterName).defaultTransport().globalJmxStatistics().disable().shutdown().hookBehavior(ShutdownHookBehavior.DONT_REGISTER);			
			if(serializer!=null) {
				if(!(serializer instanceof JbossSerializer))
					gcBuilder.serialization().marshaller(new InfinispanMarshaller(serializer));
				else
					logger.warn("Going to ignore serializer settings. The selected type is jboss however its used as native for infinispan");
			}
			
			gcBuilder.serialization().addAdvancedExternalizer(AbstractTreeSegment.Externalizer.EXTERNALIZER_ID, new AbstractTreeSegment.Externalizer());
			gcBuilder.serialization().addAdvancedExternalizer(ClusteredUUIDExternalizer.EXTERNALIZER_ID, new ClusteredUUIDExternalizer());
			gcBuilder.serialization().addAdvancedExternalizer(ClusteredIDAndStringKey.EXTERNALIZER_ID, new ExternalizableExternalizer(ClusteredIDAndStringKey.class,ClusteredIDAndStringKey.EXTERNALIZER_ID));
			gcBuilder.serialization().addAdvancedExternalizer(StringAndClusteredIDKey.EXTERNALIZER_ID, new ExternalizableExternalizer(StringAndClusteredIDKey.class,StringAndClusteredIDKey.EXTERNALIZER_ID));
			gcBuilder.serialization().addAdvancedExternalizer(MultiStringKey.EXTERNALIZER_ID, new ExternalizableExternalizer(MultiStringKey.class,MultiStringKey.EXTERNALIZER_ID));			
			GlobalConfiguration globalConfig = gcBuilder.build();			
			jBossCacheContainer=new DefaultCacheManager(globalConfig, defaultConfig.build(), false);
		}
		else {				
			Configuration defaultLocalConfig = new ConfigurationBuilder().invocationBatching().enable().clustering().cacheMode(CacheMode.LOCAL).transaction().transactionManagerLookup(txLookup).lockingMode(LockingMode.PESSIMISTIC).locking().isolationLevel(IsolationLevel.READ_COMMITTED).lockAcquisitionTimeout(aquireTimeout).useLockStriping(false).jmxStatistics().disable().build();
			GlobalConfiguration globalLocalConfig = new GlobalConfigurationBuilder().defaultCacheName("slee-default-local").globalJmxStatistics().disable().shutdown().hookBehavior(ShutdownHookBehavior.DONT_REGISTER).build();
			jBossCacheContainer=new DefaultCacheManager(globalLocalConfig, defaultLocalConfig, false);			
		}
					
		return jBossCacheContainer;
    }
    
    public void startCache() {
        if (isStarted.compareAndSet(false, true)) {
            logger.info("Starting Restcomm Cache " + name + " ...");
            AdvancedCache<?,?> cache;
            AdvancedCache<TreeSegment<?>, AtomicMap<Object,Object>> internalCache=null;
            if(isTree) {
            	Cache<TreeSegment<?>, AtomicMap<Object,Object>> realCache=jBossCacheContainer.getCache(name);
            	internalCache=realCache.getAdvancedCache();
            	cache=internalCache.getAdvancedCache();
            }
            else {
            	this.jbossCache = jBossCacheContainer.getCache(name).getAdvancedCache();
            	cache=this.jbossCache;  
            	
            	if(!localMode) {
            		try {
            			counterManager = EmbeddedCounterManagerFactory.asCounterManager(jBossCacheContainer);
            		}
            		catch(NullPointerException ex) {
            			logger.warn("Counter Manager is null, will use local counters");
            		}
            		
            		try {
            			lockManager = EmbeddedClusteredLockManagerFactory.from(jBossCacheContainer);
            		}
            		catch(NullPointerException ex) {
            			logger.warn("Counter Manager is null, will use local counters");
            		}
            	}
            }
            
            if(isTree) {
	            TreeCacheFactory tcf = new TreeCacheFactory();
	            this.treeCache = tcf.createTreeCache(internalCache,cacheDataExecutorService.getThreads(),generator,logStats);
	            this.treeCache.start();
            } 
            else
            	this.jbossCache.start();                        
            
            if (logger.isInfoEnabled())
                logger.info("Restcomm Cache  " + name + " started, status: " + cache.getStatus() + ", Mode: "
                        + cache.getCacheConfiguration().clustering().cacheMode());
        }
    }

    public String getLocalAddress() {
    	return jBossCacheContainer.getNodeAddress();    	
    }

    public AdvancedCache<Object, Object> getNonTreeCache() {    	
    	if(jbossCache==null)
    		startCache();
    	
        return jbossCache;
    }
    
    public TreeCache getTreeCache() {    	
    	if(treeCache==null)
    		startCache();
    	
        return treeCache;
    }

    public boolean isBuddyReplicationEnabled() {
        // only for JBoss Cache based RestcommCache
        return false;
    }

    public void setForceDataGravitation(boolean enableDataGravitation) {
        // only for JBoss Cache based RestcommCache    	
    }

    /*public TransactionManager getTxManager() {
        return cache.getAdvancedCache().getTransactionManager();
    }*/

    /**
     * Indicates if the cache is not in a cluster environment.
     *
     * @return the localMode
     */
    public boolean isLocalMode() {
    	return localMode;
    }

    public Object get(RestcommCluster cluster,Object key,Boolean ignoreRollbackState) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	Object result = null;
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    	    result = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).get(key);
    	} else {
            result = cacheDataExecutorService.get(cluster, key);
        }

        return result;
	}

    public void getAsync(RestcommCluster cluster,Object key,AsyncCacheCallback<Object> callback) {
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	CompletableFuture<Object> future = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).getAsync(key);
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(r);
    	});
	}

    public Boolean exists(RestcommCluster cluster,Object key,Boolean ignoreRollbackState) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	boolean result = false;
        if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
            result = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).containsKey(key);
        } else {
            Boolean exists = cacheDataExecutorService.exists(cluster, key);
            if (exists != null) {
                result = exists;
            }
        }
    	return result;
    }

    public void existsAsync(RestcommCluster cluster,Object key,AsyncCacheCallback<Boolean> callback) {
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	CompletableFuture<Boolean> future = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).containsKeyAsync(key);
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(r);
    	});
    }
    
    public Object remove(RestcommCluster cluster,Object key,Boolean ignoreRollbackState,Boolean returnValue) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	// MAY BE USEFULL TO CONTROLL THE LOCKING FROM CODE IN FUTURE
        /*
         * try { if(this.cache.getJBossCache().getAdvancedCache().getTransactionManager().getTransaction()!=null) {
         * this.cache.getJBossCache().getAdvancedCache().lock(key); } } catch(SystemException ex) {
         * 
         * }
         */
    	
    	Object result = null;
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		if(returnValue)    			
    			result = getNonTreeCache().remove(key);
    		else
    			getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).remove(key);    		    		
    	}
    	else {
    		if(!isCurrentTransactionInRollback())
    			cacheDataExecutorService.remove(cluster, key, returnValue);
    		else if(returnValue)
    			result = get(cluster,key,ignoreRollbackState);    		
    	}
    	
    	return result;
    }
    
    public void removeAsync(RestcommCluster cluster,Object key,Boolean returnValue,AsyncCacheCallback<Object> callback) {
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	CompletableFuture<Object> future;
    	if(returnValue)
    		future = getNonTreeCache().removeAsync(key);
		else
			future = getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).removeAsync(key);    
    	
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(r);
    	});
    }
    
    public void put(RestcommCluster cluster,Object key,Object value,Long maxIdleMs,Boolean ignoreRollbackState) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	// MAY BE USEFULL TO CONTROLL THE LOCKING FROM CODE IN FUTURE
        /*
         * try { if(this.cache.getJBossCache().getAdvancedCache().getTransactionManager().getTransaction()!=null) {
         * this.cache.getJBossCache().getAdvancedCache().lock(key); } } catch(SystemException ex) {
         * 
         * }
         */

        /*
         * Infinispan returns invalid state exception while expecting to do nothing on set there modifying the logic to simply
         * ignore rolledback transaction
         */
            	
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) { 
    		if(maxIdleMs!=null)
    			getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(key, value, -1, TimeUnit.MILLISECONDS, maxIdleMs, TimeUnit.MILLISECONDS);
    		else
    			getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(key, value);    		
    	} else if(!isCurrentTransactionInRollback())
    		cacheDataExecutorService.put(cluster, key, value, maxIdleMs);
    }
    
    public void putAsync(RestcommCluster cluster,Object key,Object value,Long maxIdleMs,AsyncCacheCallback<Void> callback) {
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	CompletableFuture<Object> future;
    	if(maxIdleMs!=null)
    		future = getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(key, value, -1, TimeUnit.MILLISECONDS, maxIdleMs, TimeUnit.MILLISECONDS);
    	else
    		future = getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(key, value);
    	
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(null);
    	});
    }
    
    public Boolean putIfAbsent(RestcommCluster cluster,Object key,Object value, Long maxIdleMs, Boolean ignoreRollbackState) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	// MAY BE USEFULL TO CONTROLL THE LOCKING FROM CODE IN FUTURE
        /*
         * try { if(this.cache.getJBossCache().getAdvancedCache().getTransactionManager().getTransaction()!=null) {
         * this.cache.getJBossCache().getAdvancedCache().lock(key); } } catch(SystemException ex) {
         * 
         * }
         */

        /*
         * Infinispan returns invalid state exception while expecting to do nothing on set there modifying the logic to simply
         * ignore rolledback transaction
         */
        
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {  
    		if(maxIdleMs!=null)
    			return getNonTreeCache().putIfAbsent(key, value, -1, TimeUnit.MILLISECONDS, maxIdleMs, TimeUnit.MILLISECONDS)==null;
    		else
    			return getNonTreeCache().putIfAbsent(key, value)==null;
    		
    	} else if(!isCurrentTransactionInRollback())
    		return cacheDataExecutorService.putIfAbsent(cluster, key, value, maxIdleMs);
    	
    	return false;
    }
    
    public void putIfAbsentAsync(RestcommCluster cluster,Object key,Object value,Long maxIdleMs,AsyncCacheCallback<Boolean> callback) {
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	CompletableFuture<Object> future;
    	if(maxIdleMs!=null)
    		future = getNonTreeCache().putIfAbsentAsync(key, value, -1, TimeUnit.MILLISECONDS, maxIdleMs, TimeUnit.MILLISECONDS);
    	else
    		future = getNonTreeCache().putIfAbsentAsync(key, value);
    	
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(r==null);
    	});
    }

    /*
     * Retreives count of the keys stored in specific cache beware of using this operation , its very very expensive
     */
    public Integer getCount() {
    	return getNonTreeCache().withFlags(Flag.SKIP_LOCKING).keySet().size();        
    }
    
    /*
     * Clears the cache storage completely
     */
    public void clear() {
    	getNonTreeCache().clear();
    }
    
    /*
     * Clears the cache storage completely asynchronously
     */
    public void clearAsync(AsyncCacheCallback<Void> callback) {
    	CompletableFuture<Void> future = getNonTreeCache().clearAsync();
    	future.whenCompleteAsync((r, t) -> {
    		if(t!=null)
    			callback.onError(t);
    		else
    			callback.onSuccess(r);
    	});    	    	
    }
    
    /*
     * Retreives all the keys stored in specific cache beware of using this operation , its very very expensive
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Set getAllKeys() {
    	CloseableIterator<Object> values = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).keySet().iterator();
        Set output = new HashSet();
        while (values.hasNext()) {
            output.add(values.next());
        }

        
        return output;
    }
    
    /*
     * Retreives all the keys stored in specific cache beware of using this operation , its very very expensive
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Map getAllElements() {
    	CloseableIterator<Map.Entry<Object, Object>> values = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).entrySet().iterator();
        Map output = new HashMap();
        while (values.hasNext()) {
            Map.Entry<Object, Object> curr = values.next();
            output.put(curr.getKey(), curr.getValue());
        }

        return output;
    }

    public void addListener(Object listener) {
    	if(jbossCache!=null)
    		jbossCache.addListener(listener);
    	else
    		treeCache.getCache().addListener(listener);
    }

    public List<String> getCurrentView() {
    	List<Address> members;
    	if(jbossCache!=null)
    		members=jbossCache.getCacheManager().getMembers();
    	else
    		members=treeCache.getCache().getCacheManager().getMembers();
    	
    	List<String> membersValues=new ArrayList<String>();
    	if(members!=null) {
    		for(Address curr:members)
    			membersValues.add(curr.toString());
    	}
    	
        return new ArrayList<String>(membersValues);
    }

    protected CacheDataExecutorService getCacheDataExecutorService() {
        return this.cacheDataExecutorService;
    }
    
    public String getName() {
        return this.name;
    }

    public void stopCache() {
    	if (logger.isInfoEnabled()) {
            logger.info("Restcomm Cache " + name + " stopping...");
        }
    	
    	if(jbossCache!=null) {
    		this.jbossCache.stop();
    	}
    	else if(treeCache!=null)
    		this.treeCache.stop();
    	
        if (logger.isInfoEnabled()) {
            logger.info("Restcomm Cache " + name + " stopped.");
        }
    }
    
    public List<TreeSegment<?>> getAllChilds(RestcommCluster cluster,TreeSegment<?> key,Boolean ignoreRollbackState)
	{
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	List<TreeSegment<?>> output=new ArrayList<TreeSegment<?>>();
    	ConcurrentHashMap<Object, Boolean> usedObjects=new ConcurrentHashMap<Object, Boolean>();
    	
    	if (!isTransactionUnavailable()) {
    		ConcurrentHashMap<TreeSegment<?>, TreeTxState> txMap;
    		if(key!=null && key instanceof RootTreeSegment)
    			txMap=retreiveTxStateChilds(null);
    		else
    			txMap=retreiveTxStateChilds(key);
    		    		
    		if(txMap!=null) {
    			Iterator<Entry<TreeSegment<?>, TreeTxState>> iterator=txMap.entrySet().iterator();
	    		while(iterator.hasNext()) {
	    			Entry<TreeSegment<?>, TreeTxState> currEntry=iterator.next();
	    			if(currEntry.getValue().getOperation()!=WriteOperation.REMOVE && currEntry.getValue().getOperation()!=WriteOperation.REMOVE_VALUE && !(currEntry.getValue().getOperation()==WriteOperation.NOOP && currEntry.getValue().getData()==null)) {
	    				output.add(currEntry.getKey());
    				}
    				
	    			usedObjects.put(currEntry.getKey().getSegment(),true);
	    		}
    		} 
    		
    		if(key!=null && !(key instanceof RootTreeSegment)) {
    			TreeTxState txState=retreiveTxState(key, true);
    			if(txState!=null && txState.isPreloaded())
    				return output;
    		}
    	}
    		 
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		Set<TreeSegment<?>> result=null;
    		Node currNode=getTreeCache().getNode(key,Flag.SKIP_LOCKING);
			if(currNode!=null)
				result = currNode.getChildrenNames(Flag.SKIP_LOCKING);
						
			if(result!=null) {
				for(TreeSegment<?> currSegment:result) {
	    			if(!usedObjects.containsKey(currSegment.getSegment())) {
	    				output.add(currSegment);
	    			}
	    		}       	 
    		}
    	} else {
    		List<TreeSegment<?>> cacheResponse=cacheDataExecutorService.treeGetChilds(cluster, key);
    		for(TreeSegment<?> currSegment:cacheResponse) {
    			if(!usedObjects.containsKey(currSegment.getSegment())) {
    				output.add(currSegment);
    			}
    		}
        }
    	
    	return output;
    }
    
    public void getAllChildsAsync(RestcommCluster cluster,TreeSegment<?> key,AsyncCacheCallback<List<TreeSegment<?>>> callback)
	{
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().getNodeAsync(key,new AsyncCacheCallback<Node>() {
			
			@Override
			public void onSuccess(Node value) {
				List<TreeSegment<?>> output=new ArrayList<TreeSegment<?>>();
		    	ConcurrentHashMap<Object, Boolean> usedObjects=new ConcurrentHashMap<Object, Boolean>();
		    	Set<TreeSegment<?>> result=null;
		    	if(value!=null)
					result = value.getChildrenNames(Flag.SKIP_LOCKING);
							
				if(result!=null) {
					for(TreeSegment<?> currSegment:result) {
		    			if(!usedObjects.containsKey(currSegment.getSegment())) {
		    				output.add(currSegment);
		    			}
		    		}       	 
				}
				
				callback.onSuccess(output);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		},Flag.SKIP_LOCKING);				
    }
    
    public Boolean hasChildren(RestcommCluster cluster,TreeSegment<?> key,Boolean ignoreRollbackState)
	{
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	if (!isTransactionUnavailable()) {
    		ConcurrentHashMap<TreeSegment<?>, TreeTxState> txMap;
    		if(key!=null && key instanceof RootTreeSegment)
    			txMap=retreiveTxStateChilds(null);
    		else
    			txMap=retreiveTxStateChilds(key);
    		    		
    		if(txMap!=null) {
    			Iterator<Entry<TreeSegment<?>, TreeTxState>> iterator=txMap.entrySet().iterator();
	    		while(iterator.hasNext()) {
	    			Entry<TreeSegment<?>, TreeTxState> currEntry=iterator.next();
	    			if(currEntry.getValue().getOperation()!=WriteOperation.REMOVE && currEntry.getValue().getOperation()!=WriteOperation.REMOVE_VALUE && !(currEntry.getValue().getOperation()==WriteOperation.NOOP && currEntry.getValue().getData()==null)) {
	    				return true;
    				}    				
	    		}
    		} 
    		
    		if(key!=null && !(key instanceof RootTreeSegment)) {
    			TreeTxState txState=retreiveTxState(key, true);
    			if(txState!=null && txState.isPreloaded())
    				return false;
    		}	
    	}
    		 
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		Node currNode=getTreeCache().getNode(key,Flag.SKIP_LOCKING);
			if(currNode!=null)
				 return currNode.hasChildren(Flag.SKIP_LOCKING);						
    	} else {
    		return cacheDataExecutorService.treeHasChilds(cluster, key);    		
        }
    	
    	return false;
    }
    
    public void hasChildrenAsync(RestcommCluster cluster,TreeSegment<?> key,AsyncCacheCallback<Boolean> callback)
	{
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().getNodeAsync(key,new AsyncCacheCallback<Node>() {
			
			@Override
			public void onSuccess(Node value) {
				if(value!=null)
					callback.onSuccess(value.hasChildren(Flag.SKIP_LOCKING));
				else
					callback.onSuccess(false);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		},Flag.SKIP_LOCKING);
    }
    
    public Map<TreeSegment<?>,Object> getAllChildrenData(RestcommCluster cluster,TreeSegment<?> key,Boolean ignoreRollbackState)
	{
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	Map<TreeSegment<?>,Object> childrenMap=new HashMap<TreeSegment<?>, Object>();
    	ConcurrentHashMap<Object, Boolean> usedObjects=new ConcurrentHashMap<Object, Boolean>();
    	
    	if (!isTransactionUnavailable()) {
    		ConcurrentHashMap<TreeSegment<?>, TreeTxState> txMap;
    		if(key!=null && key instanceof RootTreeSegment)
    			txMap=retreiveTxStateChilds(null);
    		else
    			txMap=retreiveTxStateChilds(key);
    		
    		if(txMap!=null) {
	    		Iterator<Entry<TreeSegment<?>, TreeTxState>> iterator=txMap.entrySet().iterator();
	    		while(iterator.hasNext()) {
	    			Entry<TreeSegment<?>, TreeTxState> currEntry=iterator.next();
	    			if(currEntry.getValue().getOperation()!=WriteOperation.REMOVE && currEntry.getValue().getOperation()!=WriteOperation.REMOVE_VALUE && !(currEntry.getValue().getOperation()==WriteOperation.NOOP && currEntry.getValue().getData()==null)) {
	    				childrenMap.put(currEntry.getKey(),currEntry.getValue().getData());
    				}
    				
	    			usedObjects.put(currEntry.getKey().getSegment(),true);
	    		}
    		} 
    		
    		if(key!=null && !(key instanceof RootTreeSegment)) {
    			TreeTxState txState=retreiveTxState(key, true);
    			if(txState!=null && txState.isPreloaded())
    				return childrenMap;
    		}
    	}
		
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		Node currNode=getTreeCache().getNode(key,Flag.SKIP_LOCKING);
    		if(currNode!=null)
    		{
				Set<Entry<TreeSegment<?>,Object>> result = currNode.getChildren(Flag.SKIP_LOCKING);
	    		if(result!=null) {
	    			for(Entry<TreeSegment<?>,Object> currEntry:result) {
	    				if(!usedObjects.containsKey(currEntry.getKey().getSegment())) {
		    	    		childrenMap.put(currEntry.getKey(),currEntry.getValue());		    				
	    				}
	    			}
	    		}
    		}
    	} else {
    		Map<TreeSegment<?>, Object> cacheResponse = cacheDataExecutorService.treeGetChildrensData(cluster, key);
    		Iterator<Entry<TreeSegment<?>, Object>> iterator=cacheResponse.entrySet().iterator();
    		while(iterator.hasNext()) {
    			Entry<TreeSegment<?>, Object> currEntry=iterator.next();
    			if(!usedObjects.containsKey(currEntry.getKey().getSegment()))
    				childrenMap.put(currEntry.getKey(), currEntry.getValue());
    		}
        }

    	return childrenMap;
    }
    
    public void getAllChildrenDataAsync(RestcommCluster cluster,TreeSegment<?> key,AsyncCacheCallback<Map<TreeSegment<?>,Object>> callback)
	{
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().getNodeAsync(key,new AsyncCacheCallback<Node>() {			
			@Override
			public void onSuccess(Node value) {
				Map<TreeSegment<?>,Object> childrenMap=new HashMap<TreeSegment<?>, Object>();
		    	ConcurrentHashMap<Object, Boolean> usedObjects=new ConcurrentHashMap<Object, Boolean>();
		    	
		    	if(value!=null)
	    		{
					Set<Entry<TreeSegment<?>,Object>> result = value.getChildren(Flag.SKIP_LOCKING);
		    		if(result!=null) {
		    			for(Entry<TreeSegment<?>,Object> currEntry:result) {
		    				if(!usedObjects.containsKey(currEntry.getKey().getSegment())) {
			    	    		childrenMap.put(currEntry.getKey(),currEntry.getValue());		    				
		    				}
		    			}
		    		}
	    		}
		    	
		    	callback.onSuccess(childrenMap);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		},Flag.SKIP_LOCKING);
    }
    
    public Object treeGet(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState) 
	{
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	Object result = null;
    	//if (!isTransactionUnavailable()) {
			TreeTxState state=retreiveTxState(key,true);
			if(state!=null) {
				switch (state.getOperation()) {
					case CREATE:
					case SET:
						return state.getData();										
					case REMOVE:
					case REMOVE_VALUE:
							return null;			
					case NOOP:
					default:
						if(state.getData()!=null) 
							return state.getData();
						break;					
				}
			}
			
			Boolean isFirstParent=true;
			TreeSegment<?> currKey=key;
			while(!(currKey.getParent() instanceof RootTreeSegment))
			{
				currKey=currKey.getParent();
				state=retreiveTxState(currKey,true);
				if(state!=null) {
		        	switch (state.getOperation()) {
						case CREATE:
						case SET:
						case REMOVE:
						case REMOVE_VALUE:
							return null;							
						case NOOP:
							if(isFirstParent && state.isPreloaded())
								return null;
							break;
						default:
							break;			
					}
				}
				
				isFirstParent=false;
			}
		//}
		
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		//result = getTreeCache().get(retreiveKey(key), "v", Flag.SKIP_LOCKING);    		
    		Node node=getTreeCache().getNode(key.getParent(), Flag.SKIP_LOCKING);
    		if(node!=null)
    			result = node.get(key,Flag.SKIP_LOCKING);	
    	} else {
            result = cacheDataExecutorService.treeGet(cluster, key);
        }

    	return result;
	}
    
    public void treeGetAsync(RestcommCluster cluster,TreeSegment<?> key,AsyncCacheCallback<Object> callback) 
	{
    	if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			
			@Override
			public void onSuccess(Node value) {
				if(value!=null)
					callback.onSuccess(value.get(key,Flag.SKIP_LOCKING));
				else
					callback.onSuccess(null);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		},Flag.SKIP_LOCKING);
	}

	public Boolean treeExists(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		boolean result = false;
		//if (!ignoreRollbackState && !isTransactionUnavailable()) {
			TreeTxState state=retreiveTxState(key,true);
			if(state!=null) {
	        	switch (state.getOperation()) {
					case CREATE:
					case SET:
						return true;
					case REMOVE:
					case REMOVE_VALUE:
						return false;							
					case NOOP:
					default:
						if(state.getData()!=null)
							return true;
						
						if(state.isPreloaded() && state.hasRealChilds())
							return true;
						
						break;
				}	        		        	
			}
			
			Boolean isFirstParent=true;
			TreeSegment<?> currKey=key;
			while(!(currKey.getParent() instanceof RootTreeSegment))
			{
				currKey=currKey.getParent();
				state=retreiveTxState(currKey,true);
				if(state!=null) {
		        	switch (state.getOperation()) {
						case CREATE:
						case SET:
						case REMOVE:
						case REMOVE_VALUE:
							return false;							
						case NOOP:
							if(isFirstParent && state.isPreloaded())
								return false;							
						default:
							break;			
					}
				}
				
				isFirstParent=false;
			}
		//}
		
        if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
        	result = getTreeCache().exists(key, Flag.SKIP_LOCKING);
        } else {
            Boolean exists = cacheDataExecutorService.treeExists(cluster, key);
            if (exists != null) {
                result = exists;
            }
        }
    	
        return result;
	}

	public void treeExistsAsync(RestcommCluster cluster,TreeSegment<?> key,AsyncCacheCallback<Boolean> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().existsAsync(key, callback, Flag.SKIP_LOCKING);
	}
	
	public void treeRemove(RestcommCluster cluster, TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
			if(!ignoreRollbackState && !isTransactionUnavailable()) {
				registerTxForChanges(cluster);
    			TreeTxState txState=retreiveTxState(key,true);
    			if(txState!=null) {
    				txState.updateOperation(txState.createdInThisTX(),WriteOperation.REMOVE, null);
    				txState.clearChilds();
    			}				
    		} else {
    			if(!useExecutor) {    
    				Node node=getTreeCache().getNode(key.getParent());
    				if(node!=null) {
        				node.removeChild(key);        				
        			}
    			}
    			else {
    				cacheDataExecutorService.treeRemove(cluster, key);
    			}
    		}
    	}   	
	}
	
	public void treeRemoveAsync(RestcommCluster cluster, TreeSegment<?> key,AsyncCacheCallback<Void> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
    	getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			
			@Override
			public void onSuccess(Node value) {
				if(value!=null) {
					value.removeChild(key);   
					callback.onSuccess(null);
				}
				else
					callback.onSuccess(null);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		});
	}
	
	public void treeRemoveValue(RestcommCluster cluster, TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
			if(!ignoreRollbackState && !isTransactionUnavailable()) {
				registerTxForChanges(cluster);
    			TreeTxState txState=retreiveTxState(key,true);
    			if(txState!=null) {
    				txState.updateOperation(txState.createdInThisTX(),WriteOperation.REMOVE_VALUE, null);
    				txState.clearChilds();    				
    			}
    		} else {
    			if(!useExecutor) {    
					Node node=getTreeCache().getNode(key.getParent());
    				if(node!=null) {
    					 node.remove(key);        				
        			}
    			}    			
    			else {
    				cacheDataExecutorService.treeRemoveValue(cluster, key);
    			}
    		}
    	}
	}
	
	public void treeRemoveValueAsync(RestcommCluster cluster, TreeSegment<?> key,AsyncCacheCallback<Void> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
		getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			
			@Override
			public void onSuccess(Node value) {
				if(value!=null) {
					value.remove(key);   
					callback.onSuccess(null);
				}
				else
					callback.onSuccess(null);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		});
	}

	public Boolean treePut(RestcommCluster cluster,TreeSegment<?> key, Object value, Boolean ignoreRollbackState,Boolean useExecutor) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		Boolean result=false;
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		if(!ignoreRollbackState && !isTransactionUnavailable()) {
	    		registerTxForChanges(cluster);
	    		TreeTxState currState=retreiveTxState(key,true);
	    		if(currState==null)
	    			return false;
	    		
	    		Boolean createdInThisTX=(currState.getOperation()==WriteOperation.NOOP || currState.createdInThisTX());
	    		currState.updateOperation(createdInThisTX,WriteOperation.SET, value);
	    		return true;
    		}
    		else {
    			if(!useExecutor) {
    				Node node=getTreeCache().getNode(key.getParent());
        			if(node!=null) {
        				node.put(key, value, Flag.IGNORE_RETURN_VALUES);
        				result=true;
        			}
    				//getTreeCache().put(retreiveKey(key), "v", value);
    			}
    			else
    				result = cacheDataExecutorService.treePut(cluster, key, value);    			 	    
    		}
    	}
		
		return result;    	
	}
	
	public void treePutAsync(RestcommCluster cluster,TreeSegment<?> key, Object value,AsyncCacheCallback<Boolean> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
		getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			@Override
			public void onSuccess(Node node) {
				if(node!=null) {
					node.put(key, value, Flag.IGNORE_RETURN_VALUES);
					callback.onSuccess(true);
				}
				else
					callback.onSuccess(false);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		});
	}

	public TreePutIfAbsentResult treePutIfAbsent(RestcommCluster cluster,TreeSegment<?> key, Object value, Boolean ignoreRollbackState,Boolean useExecutor) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    		if(!ignoreRollbackState && !isTransactionUnavailable()) {
	    		registerTxForChanges(cluster);
	    		TreeTxState currState=retreiveTxState(key,true);
	    		if(currState==null)
	    			return TreePutIfAbsentResult.PARENT_NOT_FOUND;	    					
	    			
	    		if(currState.getOperation()==WriteOperation.CREATE || currState.getOperation()==WriteOperation.SET)
	    			return TreePutIfAbsentResult.ALREADY_EXISTS;
	    		
	    		Boolean createdInThisTX=(currState.getOperation()==WriteOperation.NOOP || currState.createdInThisTX());
	    		currState.updateOperation(createdInThisTX,WriteOperation.SET, value);
	    		return TreePutIfAbsentResult.OK;
    		}
    		else {
    			if(!useExecutor) {
    				Node node=getTreeCache().getNode(key.getParent());
    				if(node==null)
    					return TreePutIfAbsentResult.PARENT_NOT_FOUND;
    							
        			if(node.putIfAbsent(key, value)!=null)
        				return TreePutIfAbsentResult.ALREADY_EXISTS;
    			}
    			else
    				return cacheDataExecutorService.treePutIfAbsent(cluster, key, value);    			 	    
    		}
    	}	
		
		return TreePutIfAbsentResult.OK;
	}
	
	public void treePutIfAbsentAsync(RestcommCluster cluster,TreeSegment<?> key, Object value,AsyncCacheCallback<TreePutIfAbsentResult> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
		getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			@Override
			public void onSuccess(Node node) {
				if(node!=null) {
					if(node.putIfAbsent(key, value)!=null)
						callback.onSuccess(TreePutIfAbsentResult.ALREADY_EXISTS);
					else
						callback.onSuccess(TreePutIfAbsentResult.OK);
				}
				else
					callback.onSuccess(TreePutIfAbsentResult.PARENT_NOT_FOUND);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		});
	}
	
	public Boolean treeCreate(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		Boolean result=false;
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
			if(!ignoreRollbackState && !isTransactionUnavailable()) {
	        	registerTxForChanges(cluster);
	    		TreeTxState currState=retreiveTxState(key,true);
    			if(currState==null)
    		    	return false;
    			
    	    	if(currState.getOperation()!=WriteOperation.REMOVE && currState.getOperation()!=WriteOperation.REMOVE_VALUE && currState.getOperation()!=WriteOperation.NOOP)
    				return false;
    			
		    	if(currState.getOperation()==WriteOperation.NOOP && treeExists(cluster, key, ignoreRollbackState))
    				return false;
    			
		    	Boolean createdInThisTX=(currState.getOperation()==WriteOperation.NOOP || currState.createdInThisTX());
    			currState.updateOperation(createdInThisTX,WriteOperation.CREATE, currState.getData());
    			currState.setIsPreloaded(true);
    			result=true;    			
    		} else {
    	    	if(!useExecutor) {
			    	Node node=getTreeCache().getNode(key.getParent());
        			if(node!=null)  {
        				node = node.addChild(key, false);
        				result=(node!=null);
        			}
    				//getTreeCache().put(retreiveKey(key), "v", null);
    			}
    			else
    				result = cacheDataExecutorService.treeCreate(cluster, key);    			
    		}
    	}
		return result;    	
	}
	
	public void treeCreateAsync(RestcommCluster cluster,TreeSegment<?> key, AsyncCacheCallback<Boolean> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
		getTreeCache().getNodeAsync(key.getParent(),new AsyncCacheCallback<Node>() {
			@Override
			public void onSuccess(Node node) {
				if(node!=null) {
					node = node.addChild(key, false);
					callback.onSuccess(node!=null);					
				}
				else
					callback.onSuccess(false);
			}
			
			@Override
			public void onError(Throwable error) {
				callback.onError(error);
			}
		});
	}
	
	public Boolean treeMulti(RestcommCluster cluster,Map<TreeSegment<?>,Object> putItems,Boolean createParent,Boolean ignoreRollbackState) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	Boolean result=true;
		if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
			if(!ignoreRollbackState && !isTransactionUnavailable()) {
				if(putItems!=null && putItems.size()>0) {
					if(createParent) {
						Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
						result|=treeCreate(cluster, firstEntry.getKey().getParent(), ignoreRollbackState, false);
					}
				}
				
				if(result) {
					Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
					while(iterator.hasNext())
					{
						Entry<TreeSegment<?>, Object> currEntry=iterator.next();
						result|=treePut(cluster,currEntry.getKey(),currEntry.getValue(),ignoreRollbackState,false);
					}
				}
			}
			else
			{
				Semaphore txSemaphore=new Semaphore(0);		
				MultiTXCommand multiTxCommand=new MultiTXCommand(cluster.getTransactionManager(),txSemaphore,putItems,createParent);
				getCacheDataExecutorService().executeCommand(multiTxCommand);
				try {
					txSemaphore.acquire();
				}
				catch(InterruptedException ex) {
					
				}
			}
		}
		
		return result;
	}
	
	public void treeMultiAsync(RestcommCluster cluster,Map<TreeSegment<?>,Object> putItems,Boolean createParent,AsyncCacheCallback<Boolean> callback) {
		if(!isAsync)
    		throw new RuntimeException("Sync cache does not supports async operations");
    	
		if(putItems!=null && putItems.size()>0) {
			Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
			getTreeCache().getNodeAsync(firstEntry.getKey().getParent().getParent(),new AsyncCacheCallback<Node>() {
				
				@Override
				public void onSuccess(Node parentNode) {
					if(createParent && parentNode!=null)
						parentNode = parentNode.addChild(firstEntry.getKey().getParent(), true);	
					
					Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
					while(iterator.hasNext())
					{
						Entry<TreeSegment<?>, Object> currEntry=iterator.next();
						parentNode.put(currEntry.getKey(), currEntry.getValue(), Flag.IGNORE_RETURN_VALUES);
					}
				}
				
				@Override
				public void onError(Throwable error) {
					callback.onError(error);
				}
			});
		}
		else
			callback.onSuccess(true);
	}
	
    public void treeMarkAsPreloaded(RestcommCluster cluster,Map<TreeSegment<?>,Object> putItems) {
    	if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	if(putItems!=null && putItems.size()>0) {
    		Transaction tx=null;
    		if(txMgr==null)
    			return;
    		
    		try {
    			tx=txMgr.getTransaction();
    		} catch (SystemException e) {
    			throw new IllegalStateException("Unable to register listener for created transaction. Error: "+e.getMessage());
    		}
    		
    		if(tx==null)
    			return;
    		
    		ConcurrentHashMap<TreeSegment<?>, TreeTxState> map=TreeTransactionContextThreadLocal.getTransactionContext(name,tx);
    		if(map==null)
    			return;
    		
    		Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
    		TreeSegment<?> segment=firstEntry.getKey();
    		
    		List<TreeSegment<?>> pathElements=new ArrayList<TreeSegment<?>>();
    		//pathElements.add(segment);
    		while(segment.getParent()!=null && !segment.getParent().isRoot()) {
    			pathElements.add(0, segment.getParent());
    			segment=segment.getParent(); 		
    		}
    		
    		TreeSegment<?> curr=pathElements.remove(0);
    		TreeTxState currState=map.get(curr);
    		if(currState==null) {
    			currState=new TreeTxState(false, WriteOperation.NOOP, null);
    			map.put(curr, currState);    			
    		}
    		
    		while(pathElements.size()>0) {
    			curr=pathElements.remove(0);
    			TreeTxState parentState=currState;
    			currState=currState.getChild(curr);
    			if(currState==null) {
    				currState=new TreeTxState(false, WriteOperation.NOOP, null);
    				parentState.addChild(curr, currState);    				
    			}
    		}
    		
    		Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
			while(iterator.hasNext())
			{
				Entry<TreeSegment<?>, Object> currEntry=iterator.next();
				TreeTxState childState=new TreeTxState(false, WriteOperation.NOOP, currEntry.getValue());
				currState.addChild(currEntry.getKey(), childState);  												
			}
			
			currState.setIsPreloaded(true);
		}
	}
		
	public void preload(RestcommCluster cluster,TreeSegment<?> key) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
		if(isTransactionUnavailable())
			return;
		
		registerTxForChanges(cluster);
		TreeTxState state=retreiveTxState(key,true);
		if(state==null || state.isPreloaded())
			return;
		
		if(state.getOperation()==WriteOperation.REMOVE || state.getOperation()==WriteOperation.REMOVE_VALUE || state.getOperation()==WriteOperation.CREATE) {
			//the parent is deleted anyway or just created
			state.setIsPreloaded(true);
			return;
		}
		
		Map<TreeSegment<?>, Object> allChilds=getAllChildrenData(cluster, key, false);
		if(allChilds!=null) {
			Iterator<Entry<TreeSegment<?>, Object>> iterator=allChilds.entrySet().iterator();
			while(iterator.hasNext()) {
				Entry<TreeSegment<?>, Object> currEntry=iterator.next();
				TreeTxState childState=state.getChild(currEntry.getKey());
				//add only childrens that are not in tx tree yet
				if(childState==null || childState.getOperation()==WriteOperation.NOOP)
					state.addChild(currEntry.getKey(), new TreeTxState(false, WriteOperation.NOOP, currEntry.getValue()));
			}
		}
		
		state.setIsPreloaded(true);
	}
	
	public Boolean isPreloaded(TreeSegment<?> key) {
		if(isAsync)
    		throw new RuntimeException("Async cache does not supports sync operations");
    	
    	if(isTransactionUnavailable())
			return false;
		
		TreeTxState state=retreiveTxState(key,true);
		if(state!=null)
			return state.isPreloaded();
		
		return false;
	}
	
	protected Node getParentNodeInternal(Node parent,TreeSegment<?> key) {
		if(parent!=null)
			return parent;
		
		return getTreeCache().getNode(key.getParent());
	}
	
	protected Node getNodeInternal(Node parent,TreeSegment<?> key) {
		if(parent!=null)
			return parent.getChild(key);
		
		return getTreeCache().getNode(key);
	}
	
	protected Node treeCreateInternal(Node parent, TreeSegment<?> key) {
		Node node=getParentNodeInternal(parent,key);
		if(node!=null)  {
			return node.addChild(key, true);			
		}
		
		return null;
	}

	protected void treeRemoveInternal(Node parent, TreeSegment<?> key) {
		Node node=getParentNodeInternal(parent,key);
		if(node!=null) {
			node.removeChild(key);
		}
	}

	protected void treeRemoveValueInternal(Node parent, TreeSegment<?> key) {
		Node node=getParentNodeInternal(parent,key);
		if(node!=null) {
			node.remove(key);
		}
	}

	protected Node treePutInternal(Node parent, TreeSegment<?> key, Object value) {
		Node node=getParentNodeInternal(parent,key);
		if(node!=null) {
			node.put(key, value, Flag.IGNORE_RETURN_VALUES);
			return node;
		}
		
		return null;
	}
	
    private boolean isTransactionUnavailable() {
        boolean result = false;
        if(txMgr==null)
        	return true;
        
        try {        	
        	//int transactionStatus = restcommCache.getJBossCache().getAdvancedCache().getTransactionManager().getStatus();
        	int transactionStatus = txMgr.getStatus();
            if (transactionStatus == Status.STATUS_NO_TRANSACTION || transactionStatus == Status.STATUS_MARKED_ROLLBACK || transactionStatus == Status.STATUS_COMMITTED) {
                result = true;
            }
        } catch (SystemException ex) {
        }
        return result;
    }
	
    private boolean isCurrentTransactionInRollbackOrCommitted() {
        boolean result = false;
        if(txMgr==null)
        	return false;
        
        try {        	
        	//int transactionStatus = restcommCache.getJBossCache().getAdvancedCache().getTransactionManager().getStatus();
        	int transactionStatus = txMgr.getStatus();
        	if (transactionStatus == Status.STATUS_MARKED_ROLLBACK || transactionStatus == Status.STATUS_COMMITTED) {
                result = true;
            }
        } catch (SystemException ex) {
        }
        return result;
    }
	
    private boolean isCurrentTransactionInRollback() {
        boolean result = false;
        if(txMgr==null)
        	return false;
        
        try {        	
        	//int transactionStatus = restcommCache.getJBossCache().getAdvancedCache().getTransactionManager().getStatus();
        	int transactionStatus = txMgr.getStatus();
        	if (transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
                result = true;
            }
        } catch (SystemException ex) {
        }
        return result;
    }
    
    private ConcurrentHashMap<TreeSegment<?>, TreeTxState> registerTxForChanges(RestcommCluster cluster) {
		Transaction tx=null;
		if(txMgr==null)
			throw new IllegalStateException("Unable to register listener for created transaction. Error: tx not found");
		
        try {
			tx=txMgr.getTransaction();
		} catch (SystemException e) {
			throw new IllegalStateException("Unable to register listener for created transaction. Error: "+e.getMessage());
		}
		
		if(tx==null)
			throw new IllegalStateException("Unable to register listener for created transaction. Error: tx not found");
		
		ConcurrentHashMap<TreeSegment<?>, TreeTxState> realMap=TreeTransactionContextThreadLocal.getTransactionContext(name,tx);
		if(realMap==null) {
			realMap=new ConcurrentHashMap<TreeSegment<?>, TreeTxState>();
			try {
				tx.registerSynchronization(new InfinispanTxSync(tx, cluster, this,realMap));							
			} catch (RollbackException | SystemException e) {
				throw new IllegalStateException("Unable to register listener for created transaction. Error: "+e.getMessage());
			}
			TreeTransactionContextThreadLocal.setTransactionContext(name,tx,realMap);			
		}
		
		return realMap;
	}
	
	private TreeTxState retreiveTxState(TreeSegment<?> segment,Boolean markNoopIfNotExists) {
		Transaction tx=null;
		if(txMgr==null)
        	return null;
        
		try {
			tx=txMgr.getTransaction();
		} catch (SystemException e) {
			throw new IllegalStateException("Unable to register listener for created transaction. Error: "+e.getMessage());
		}
		
		if(tx==null)
			return null;
		
		ConcurrentHashMap<TreeSegment<?>, TreeTxState> map=TreeTransactionContextThreadLocal.getTransactionContext(name,tx);
		if(map==null)
			return null;
		
		List<TreeSegment<?>> pathElements=new ArrayList<TreeSegment<?>>();
		pathElements.add(segment);
		while(segment.getParent()!=null && !segment.getParent().isRoot()) {
			pathElements.add(0, segment.getParent());
			segment=segment.getParent();
		}
		
		TreeSegment<?> rootSegment=pathElements.remove(0);
		TreeTxState currState=map.get(rootSegment);
		if(currState==null) {
			if(!markNoopIfNotExists)
				return null;
			else {
				currState=new TreeTxState(false, WriteOperation.NOOP, null);
				map.put(rootSegment, currState);
			}
		}
		
		while(pathElements.size()>0) {
			TreeSegment<?> curr=pathElements.remove(0);
			TreeTxState parentState=currState;
			currState=currState.getChild(curr);
			if(currState==null)
				if(!markNoopIfNotExists)
					return null;
				else {
					currState=new TreeTxState(false, WriteOperation.NOOP, null);
					parentState.addChild(curr, currState);					
				}
		}
		
		return currState; 		
	}
	
	private ConcurrentHashMap<TreeSegment<?>,TreeTxState> retreiveTxStateChilds(TreeSegment<?> segment) {
		Transaction tx=null;
		if(txMgr==null)
        	return null;
        
		try {
			tx=txMgr.getTransaction();
		} catch (SystemException e) {
			throw new IllegalStateException("Unable to register listener for created transaction. Error: "+e.getMessage());
		}
		
		if(tx==null)
			return null;
		
		ConcurrentHashMap<TreeSegment<?>, TreeTxState> map=TreeTransactionContextThreadLocal.getTransactionContext(name,tx);
		if(map==null)
			return null;
		
		if(segment==null)
			return map;
		
		List<TreeSegment<?>> pathElements=new ArrayList<TreeSegment<?>>();
		pathElements.add(segment);
		while(segment.getParent()!=null && !segment.getParent().isRoot()) {
			pathElements.add(0, segment.getParent());
			segment=segment.getParent();
		}
		
		TreeTxState currState=map.get(pathElements.remove(0));
		for(TreeSegment<?> curr:pathElements) {
			if(currState==null)
				return null;
			
			currState=currState.getChild(curr);
		}
		
		if(currState==null)
			return null;
		
		return currState.getAllChilds(); 		
	}
	
	private class MultiTXCommand implements CommonCommand {
		private TransactionManager txManager;
		private Semaphore releaseSemaphore;
		private Map<TreeSegment<?>, Object> putEntities;
		private Boolean createParent;
		public MultiTXCommand(TransactionManager txManager,Semaphore releaseSemaphore,Map<TreeSegment<?>, Object> putEntities,Boolean createParent) {
			this.txManager=txManager;
			this.releaseSemaphore=releaseSemaphore;
			this.putEntities=putEntities;
			this.createParent=createParent;
		}
		
		@Override
		public void execute() {
			if(putEntities!=null && putEntities.size()>0) {
				try {
					txManager.begin();
				}
				catch(Exception ex) {
					logger.error("Can not start transaction on mirror thread," + ex.getMessage(),ex);
				}
				
				Entry<TreeSegment<?>, Object> firstEntry=putEntities.entrySet().iterator().next();
				Node parentNode=null;
				if(createParent)
					parentNode=treeCreateInternal(null, firstEntry.getKey().getParent());
				else
					parentNode=getParentNodeInternal(null, firstEntry.getKey().getParent());
				
				Iterator<Entry<TreeSegment<?>, Object>>  iterator=putEntities.entrySet().iterator();
				while(iterator.hasNext())
				{
					Entry<TreeSegment<?>, Object> currEntry=iterator.next();
					treePutInternal(parentNode, currEntry.getKey(), currEntry.getValue());
				}	
				
				try {
					txManager.commit();
				}
				catch(Exception ex) {
					logger.error("Can not commit transaction on mirror thread," + ex.getMessage(),ex);
					try {
						//trying to rollback so tx would not get stucked
						txManager.rollback();
					}
					catch(Exception ex1) {
						
					}
				}
			}
			
			releaseSemaphore.release();
		}

		@Override
		public void cancel() {			
		}		
	}	
	
	public Long getAtomicValue(String name) 
	{
		if(counterManager!=null) {
			if(!counterManager.isDefined(name)) {
				counterManager.defineCounter(name, counterConfiguration);
			}
			
			CompletableFuture<Long> future = counterManager.getStrongCounter(name).getValue();
			try {			
				return future.get();
			}
			catch(ExecutionException | InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldValue=localCounters.putIfAbsent(name, localValue);
				if(oldValue!=null)
					localValue=oldValue;
			}
			
			return localValue.get();
		}
	}

	public void getAtomicValueAsync(String name,AsyncCacheCallback<Long> callback) 
	{
		if(counterManager!=null) {
			if(!counterManager.isDefined(name)) {
				counterManager.defineCounter(name, counterConfiguration);
			}
			
			CompletableFuture<Long> future = counterManager.getStrongCounter(name).getValue();
			future.whenCompleteAsync((r, t) -> {
	    		if(t!=null)
	    			callback.onError(t);
	    		else
	    			callback.onSuccess(r);
	    	});
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldValue=localCounters.putIfAbsent(name, localValue);
				if(oldValue!=null)
					localValue=oldValue;
			}
			
			callback.onSuccess(localValue.get());	
		}
	}

	public Long addAndGetAtomicValue(String name, Long delta) 
	{
		if(counterManager!=null) {
			if(!counterManager.isDefined(name)) {
				counterManager.defineCounter(name, counterConfiguration);
			}
			
			CompletableFuture<Long> future = counterManager.getStrongCounter(name).addAndGet(delta);
			try {			
				return future.get();
			}
			catch(ExecutionException | InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldValue=localCounters.putIfAbsent(name, localValue);
				if(oldValue!=null)
					localValue=oldValue;
			}
			
			return localValue.addAndGet(delta);
		}
	}

	public void addAndGetAtomicValueAsync(String name, Long delta,AsyncCacheCallback<Long> callback) 
	{
		if(counterManager!=null) {
			if(!counterManager.isDefined(name)) {
				counterManager.defineCounter(name, counterConfiguration);
			}
			
			CompletableFuture<Long> future = counterManager.getStrongCounter(name).addAndGet(delta);
			future.whenCompleteAsync((r, t) -> {
	    		if(t!=null)
	    			callback.onError(t);
	    		else
	    			callback.onSuccess(r);
	    	});
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldValue=localCounters.putIfAbsent(name, localValue);
				if(oldValue!=null)
					localValue=oldValue;
			}
			
			callback.onSuccess(localValue.addAndGet(delta));	
		}
	}
	
	public Boolean compareAndSetAtomicValue(String name, Long oldValue, Long newValue) 
	{		
		if(counterManager!=null) {
			CompletableFuture<Boolean> future = counterManager.getStrongCounter(name).compareAndSet(oldValue, newValue);
			try {			
				return future.get();
			}
			catch(ExecutionException | InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldAtomic=localCounters.putIfAbsent(name, localValue);
				if(oldAtomic!=null)
					localValue=oldAtomic;
			}
			
			return localValue.compareAndSet(oldValue,newValue);
		}
	}

	public void compareAndSetAtomicValueAsync(String name, Long oldValue, Long newValue,AsyncCacheCallback<Boolean> callback) 
	{
		if(counterManager!=null) {
			CompletableFuture<Boolean> future = counterManager.getStrongCounter(name).compareAndSet(oldValue,newValue);
			future.whenCompleteAsync((r, t) -> {
	    		if(t!=null)
	    			callback.onError(t);
	    		else
	    			callback.onSuccess(r);
	    	});
		}
		else {
			AtomicLong localValue=localCounters.get(name);
			if(localValue==null) {
				localValue=new AtomicLong(0L);
				AtomicLong oldAtomic=localCounters.putIfAbsent(name, localValue);
				if(oldAtomic!=null)
					localValue=oldAtomic;
			}
			
			callback.onSuccess(localValue.compareAndSet(oldValue,newValue));
		}
	}

	public Boolean getLock(String name, Long maxWaitMS) 
	{
		if(lockManager!=null) {
			if(!lockManager.isDefined(name)) {
				lockManager.defineLock(name);
			}
			
			ClusteredLock lock = lockManager.get(name);
			CompletableFuture<Boolean> future = lock.tryLock(maxWaitMS, TimeUnit.MILLISECONDS);
			try {			
				return future.get();
			}
			catch(ExecutionException | InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
		else {
			Lock localValue=localLocks.get(name);
			if(localValue == null)
			{
				Lock newLock = new ReentrantLock();
				Lock oldLock = localLocks.putIfAbsent(name, newLock);
				if(oldLock != null)
					localValue = oldLock;
			}
			
			try {			
				return localValue.tryLock(maxWaitMS, TimeUnit.MILLISECONDS);
			}
			catch(InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
	}

	public void getLockAsync(String name, Long maxWaitMS,AsyncCacheCallback<Boolean> callback) 
	{
		if(lockManager!=null) {
			if(!lockManager.isDefined(name)) {
				lockManager.defineLock(name);
			}
			
			ClusteredLock lock = lockManager.get(name);
			CompletableFuture<Boolean> future = lock.tryLock(maxWaitMS, TimeUnit.MILLISECONDS);
			future.whenCompleteAsync((r, t) -> {
	    		if(t!=null)
	    			callback.onError(t);
	    		else
	    			callback.onSuccess(r);
	    	});
		}
		else {
			Lock localValue=localLocks.get(name);
			if(localValue == null)
			{
				Lock newLock = new ReentrantLock();
				Lock oldLock = localLocks.putIfAbsent(name, newLock);
				if(oldLock != null)
					localValue = oldLock;
			}
			
			try {			
				callback.onSuccess(localValue.tryLock(maxWaitMS, TimeUnit.MILLISECONDS));			
			}
			catch(InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
	}

	public void releaseLock(String name) 
	{
		if(lockManager!=null) {
			if(!lockManager.isDefined(name)) {
				lockManager.defineLock(name);
			}
			
			ClusteredLock lock = lockManager.get(name);
			CompletableFuture<Void> future = lock.unlock();
			try {			
				future.get();
			}
			catch(ExecutionException | InterruptedException ex) {
				throw new RuntimeException("An error occured while waiting for get atomic value result," + ex.getMessage(), ex);		    	
			}
		}
		else {
			Lock localValue=localLocks.get(name);
			if(localValue == null)
			{
				Lock newLock = new ReentrantLock();
				Lock oldLock = localLocks.putIfAbsent(name, newLock);
				if(oldLock != null)
					localValue = oldLock;
			}
			
			localValue.unlock();			
		}
	}

	public void releaseLockAsync(String name,AsyncCacheCallback<Void> callback) 
	{
		if(lockManager!=null) {
			if(!lockManager.isDefined(name)) {
				lockManager.defineLock(name);
			}
			
			ClusteredLock lock = lockManager.get(name);
			CompletableFuture<Void> future = lock.unlock();
			future.whenCompleteAsync((r, t) -> {
	    		if(t!=null)
	    			callback.onError(t);
	    		else
	    			callback.onSuccess(r);
	    	});
		}
		else {
			Lock localValue=localLocks.get(name);
			if(localValue == null)
			{
				Lock newLock = new ReentrantLock();
				Lock oldLock = localLocks.putIfAbsent(name, newLock);
				if(oldLock != null)
					localValue = oldLock;
			}
			
			localValue.unlock();			
		}
	}
}
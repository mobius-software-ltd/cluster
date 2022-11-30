/*
 * TeleStax, Open Source Cloud Communications
 * Copyright 2011-2017, Telestax Inc and individual contributors
 * by the @authors tag.
 *
 * This program is free software: you can redistribute it and/or modify
 * under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.restcomm.cache.infinispan.tree.Node;
import org.restcomm.cache.infinispan.tree.TreeCache;
import org.restcomm.cache.infinispan.tree.TreeCacheFactory;
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
    private boolean isTree;
    private String name;
    private Boolean logStats;
    
    private CacheDataExecutorService cacheDataExecutorService;
    private IDGenerator<?> generator;
    
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private DefaultCacheManager jBossCacheContainer;
    
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

		this.isTree=isTree;
        this.name = name;
        
        Thread.currentThread().setContextClassLoader(currentClassLoader);
    }

    public static DefaultCacheManager initContainer(TransactionManager txMgr,Serializer serializer,Boolean isReplicated,Boolean isParititioned,Integer copies,Integer aquireTimeout) {
    	DefaultCacheManager jBossCacheContainer;
    	TransactionManagerLookup txLookup=new TransactionManagerLookup() {				
			@Override
			public TransactionManager getTransactionManager() throws Exception {
				return txMgr;
			}
		};
		
		if(isReplicated) {
			Configuration defaultConfig;
			if(isParititioned)				
				defaultConfig = new ConfigurationBuilder().invocationBatching().enable().clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(copies).transaction().transactionManagerLookup(txLookup).lockingMode(LockingMode.PESSIMISTIC).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable().build();
			else
				defaultConfig = new ConfigurationBuilder().invocationBatching().enable().clustering().cacheMode(CacheMode.REPL_SYNC).transaction().transactionManagerLookup(txLookup).lockingMode(LockingMode.PESSIMISTIC).locking().isolationLevel(IsolationLevel.READ_COMMITTED).jmxStatistics().disable().build();
						
			GlobalConfigurationBuilder gcBuilder=new GlobalConfigurationBuilder();
			gcBuilder.defaultCacheName("slee-default").transport().clusterName("restcomm").defaultTransport().globalJmxStatistics().disable().shutdown().hookBehavior(ShutdownHookBehavior.DONT_REGISTER);			
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
			jBossCacheContainer=new DefaultCacheManager(globalConfig, defaultConfig, false);
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
    	Object result = null;
    	if (ignoreRollbackState || !isCurrentTransactionInRollbackOrCommitted()) {
    	    result = getNonTreeCache().withFlags(Flag.SKIP_LOCKING).get(key);
    	} else {
            result = cacheDataExecutorService.get(cluster, key);
        }

        return result;
	}
    
    public Boolean exists(RestcommCluster cluster,Object key,Boolean ignoreRollbackState) {
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
    
    public Object remove(RestcommCluster cluster,Object key,Boolean ignoreRollbackState,Boolean returnValue) {
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
    
    public void put(RestcommCluster cluster,Object key,Object value,Boolean ignoreRollbackState) {
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
    		getNonTreeCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(key, value);    		    	
    	} else if(!isCurrentTransactionInRollback())
    		cacheDataExecutorService.put(cluster, key, value);    	
    }
    
    public Boolean putIfAbsent(RestcommCluster cluster,Object key,Object value,Boolean ignoreRollbackState) {
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
    		return getNonTreeCache().putIfAbsent(key, value)==null;    		    	
    	} else if(!isCurrentTransactionInRollback())
    		return cacheDataExecutorService.putIfAbsent(cluster, key, value);
    	
    	return false;
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
    
    public Boolean hasChildren(RestcommCluster cluster,TreeSegment<?> key,Boolean ignoreRollbackState)
	{
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
    
    public Map<TreeSegment<?>,Object> getAllChildrenData(RestcommCluster cluster,TreeSegment<?> key,Boolean ignoreRollbackState)
	{
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
    
    public Object treeGet(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState) 
	{
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

	public Boolean treeExists(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState) {
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
						
						if(state.isPreloaded() && state.getAllChilds()!=null && state.getAllChilds().size()>0)
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

	public void treeRemove(RestcommCluster cluster, TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
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
	
	public void treeRemoveValue(RestcommCluster cluster, TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
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

	public Boolean treePut(RestcommCluster cluster,TreeSegment<?> key, Object value, Boolean ignoreRollbackState,Boolean useExecutor) {
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

	public TreePutIfAbsentResult treePutIfAbsent(RestcommCluster cluster,TreeSegment<?> key, Object value, Boolean ignoreRollbackState,Boolean useExecutor) {
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
	
	public Boolean treeCreate(RestcommCluster cluster,TreeSegment<?> key, Boolean ignoreRollbackState,Boolean useExecutor) {
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
	
	public Boolean treeMulti(RestcommCluster cluster,Map<TreeSegment<?>,Object> putItems,Boolean createParent,Boolean ignoreRollbackState) {
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
	
    public void treeMarkAsPreloaded(RestcommCluster cluster,Map<TreeSegment<?>,Object> putItems) {
    	if(putItems!=null && putItems.size()>0) {
    		Transaction tx=null;
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
}

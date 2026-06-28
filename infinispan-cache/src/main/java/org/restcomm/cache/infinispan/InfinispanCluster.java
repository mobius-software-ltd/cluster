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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.TransactionManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.restcomm.cluster.AsyncCacheCallback;
import org.restcomm.cluster.ClusterElector;
import org.restcomm.cluster.DataListener;
import org.restcomm.cluster.DataRemovalListener;
import org.restcomm.cluster.FailOverListener;
import org.restcomm.cluster.RestcommCluster;
import org.restcomm.cluster.data.CacheListener;
import org.restcomm.cluster.data.ClusterOperation;
import org.restcomm.cluster.data.RootTreeSegment;
import org.restcomm.cluster.data.TreePutIfAbsentResult;
import org.restcomm.cluster.data.TreeSegment;

/**
 * Listener that is to be used for cluster wide replication(meaning no buddy
 * replication, no data gravitation). It will index activity on nodes marking
 * current node as owner(this is semi-gravitation behavior (we don't delete, we
 * just mark)). 
 * 
 * Indexing is only at node level, i.e., there is no
 * reverse indexing, so it has to iterate through whole resource group data FQNs to check which
 * nodes should be taken over.
 * 
 * @author <a href="mailto:baranowb@gmail.com">Bartosz Baranowski </a>
 * @author martins
 * @author András Kőkuti
 * @author yulian.oifa
 */

@Listener
public class InfinispanCluster implements RestcommCluster,CacheListener {
	private static final Logger logger = LogManager.getLogger(InfinispanCluster.class);

	private final AtomicReference<FailOverListener> failOverListener=new AtomicReference<FailOverListener>(null);
	private AtomicReference<DataRemovalListener> dataRemovalListener=new AtomicReference<DataRemovalListener>(null);
	private AtomicReference<DataListener> dataListener=new AtomicReference<DataListener>(null);
	
	private ConcurrentHashMap<ClusterOperation,AtomicLong> allOperations=new ConcurrentHashMap<ClusterOperation,AtomicLong>();
	private ConcurrentHashMap<ClusterOperation,AtomicLong> operationTime=new ConcurrentHashMap<ClusterOperation,AtomicLong>();
	private ConcurrentHashMap<ClusterOperation,AtomicLong> longOperations=new ConcurrentHashMap<ClusterOperation,AtomicLong>();
	
	private final InfinispanCache localCache;
	private final TransactionManager txMgr;
	private final ClusterElector elector;
	private List<String> currentView;
	
	private boolean started;
	private boolean useRemovalOnlyListener=false;
	private ViewChangedListener viewListener;
	private Boolean logStats;
	
	public InfinispanCluster(InfinispanCache localCache, ViewChangedListener viewListener, TransactionManager txMgr, ClusterElector elector,Boolean logStats) {
		this.localCache = localCache;
		this.txMgr = txMgr;
		this.elector = elector;
		this.viewListener=viewListener;
		this.logStats=logStats;
		
		for(ClusterOperation operation:ClusterOperation.values()) {
			allOperations.put(operation, new AtomicLong(0));
			operationTime.put(operation, new AtomicLong(0));
			longOperations.put(operation, new AtomicLong(0));
		}
	}

	/* (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#getLocalAddress()
	 */
	public String getLocalAddress() {	
		return localCache.getLocalAddress();
	}
	
	/* (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#isLocalMode()
	 */
	public boolean isLocalMode() {
		return localCache.isLocalMode();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#isSingleMember()
	 */
	public boolean isSingleMember() {
		if (currentView != null)
			return currentView.size()<2;
		else
			return true;	
	}
	
	@SuppressWarnings("rawtypes")
	@CacheEntryRemoved
	public void cacheEntryRemoved(CacheEntryRemovedEvent event){
		if (logger.isDebugEnabled()) {
			logger.debug("cacheEntryRemoved : event[ "+ event +"]");
		}

		if (!event.isPre() && !event.isOriginLocal() && event.getKey() != null && event.getOldValue()!=null )
			 entryRemoved(event.getKey());
	}

	/**
	 * 
	 */
	private void performTakeOver(String lostMember, String localAddress, boolean useLocalListenerElector) {
		if (logger.isDebugEnabled()) {
			logger.debug("onViewChangeEvent : " + localAddress + " failing over lost member " + lostMember + ", useLocalListenerElector=" + useLocalListenerElector);
		}
			boolean createdTx = false;
			boolean doRollback = true;
			
			try {
				if (txMgr != null && txMgr.getTransaction() == null) {
					txMgr.begin();
					createdTx = true;
				}
				
				
				if (createdTx) {
					txMgr.commit();
					createdTx = false;
				}
				
				if (txMgr != null && txMgr.getTransaction() == null) {
					txMgr.begin();
					createdTx = true;
				}
								
				FailOverListener ftListener=failOverListener.get();
				if(ftListener!=null)
					ftListener.failOverClusterMember(lostMember);
				
				doRollback = false;
				
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
				
			} finally {
				if (createdTx) {					
					try {
						if (!doRollback) {
							txMgr.commit();
						}
						else {
							txMgr.rollback();
						}
					} catch (Exception e) {
						logger.error(e.getMessage(),e);
					}
				}
			}

	}

	// LOCAL LISTENERS MANAGEMENT
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#addFailOverListener(org.restcomm.cluster.FailOverListener)
	 */
	public boolean addFailOverListener(FailOverListener localListener) {
		if (logger.isDebugEnabled()) {
			logger.debug("Adding local listener " + localListener);
		}
		
		return failOverListener.compareAndSet(null, localListener);					
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#removeFailOverListener(org.restcomm.cluster.FailOverListener)
	 */
	public boolean removeFailOverListener(FailOverListener localListener) {
		if (logger.isDebugEnabled()) {
			logger.debug("Removing local listener " + localListener);
		}
		
		FailOverListener oldData=failOverListener.get();
		failOverListener.set(null);
		return oldData!=null;		
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#addDataRemovalListener(org.restcomm.cluster.DataRemovalListener)
	 */
	public boolean addDataRemovalListener(DataRemovalListener listener) {
		return dataRemovalListener.compareAndSet(null, listener);		
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#removeDataRemovalListener(org.restcomm.cluster.DataRemovalListener)
	 */
	public boolean removeDataRemovalListener(DataRemovalListener listener) {
		DataRemovalListener oldData=dataRemovalListener.get();
		dataRemovalListener.set(null);
		return oldData!=null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#addDataListener(org.restcomm.cluster.DataListener)
	 */
	public boolean addDataListener(DataListener listener) {
		return dataListener.compareAndSet(null, listener);		
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.restcomm.cluster.RestcommCluster#removeDataListener(org.restcomm.cluster.DataListener)
	 */
	public boolean removeDataListener(DataListener listener) {
		DataListener oldData=dataListener.get();
		dataListener.set(null);
		return oldData!=null;
	}
	
	@Override
	public void startCluster(Boolean useRemovalOnlyListener) {
		logger.info("Starting cluster");
		synchronized (this) {
			if (started) {
				throw new IllegalStateException("cluster already started");
			}
			localCache.startCache();
			
			this.useRemovalOnlyListener=useRemovalOnlyListener;
			logger.info("registring listener!");
				
			if(!isLocalMode()) {
				// get current cluster members
				currentView = localCache.getCurrentView();
				
				logger.info("setting listener for cache!");
				
				// start listening to cache events
				if(this.useRemovalOnlyListener)
					localCache.addListener(this);
				else
					localCache.addListener(new ExtendedCacheListener());
				
				viewListener.registerListener(localCache.getName(), this);	            						
			}
			
			started = true;
		}				
	}
	
	@Override
	public boolean isStarted() {
		synchronized (this) {
			return started;
		}
	}
	
	@Override
	public void stopCluster() {
		synchronized (this) {
			if (!started) {
				throw new IllegalStateException("cluster already stopped");
			}
			
			if(logStats!=null && logStats) {
				for(ClusterOperation operation:ClusterOperation.values()) {
					logger.warn(operation + " stats for " + localCache.getName() + " are:[ops=" + allOperations.get(operation).get() + ",long ops=" + longOperations.get(operation).get() + ",total time:" + operationTime.get(operation).get() + "]");				
				}
			}
			
			localCache.stopCache();
			started = false;
		}				
	}

    @Override
	public Object get(Object key,Boolean ignoreRollbackState) {
    	long startTime=System.currentTimeMillis();
		Object result=getCache().get(this, key, ignoreRollbackState);
		updateStats(ClusterOperation.GET, (System.currentTimeMillis()-startTime));
		return result;
	}

    @Override
	public void getAsync(Object key,AsyncCacheCallback<Object> callback) {
    	long startTime=System.currentTimeMillis();
		getCache().getAsync(this, key,callback);
		updateStats(ClusterOperation.GET, (System.currentTimeMillis()-startTime));		
	}
    
    @Override
	public Boolean exists(Object key,Boolean ignoreRollbackState) {
    	long startTime=System.currentTimeMillis();
		Boolean result=getCache().exists(this, key, ignoreRollbackState);
		updateStats(ClusterOperation.EXIST, (System.currentTimeMillis()-startTime));
		return result;
    }
    
    @Override
	public void existsAsync(Object key,AsyncCacheCallback<Boolean> callback) {
    	long startTime=System.currentTimeMillis();
		getCache().existsAsync(this, key, callback);
		updateStats(ClusterOperation.EXIST, (System.currentTimeMillis()-startTime));
    }
    
    @Override
    public Object remove(Object key,Boolean ignoreRollbackState,Boolean returnValue) {
    	long startTime=System.currentTimeMillis();
		Object result=getCache().remove(this, key, ignoreRollbackState, returnValue);
		updateStats(ClusterOperation.REMOVE, (System.currentTimeMillis()-startTime));
		return result;
    }
    
    @Override
    public void removeAsync(Object key,Boolean returnValue,AsyncCacheCallback<Object> callback) {
    	long startTime=System.currentTimeMillis();
		getCache().removeAsync(this, key, returnValue, callback);
		updateStats(ClusterOperation.REMOVE, (System.currentTimeMillis()-startTime));		
    }
    
    @Override
    public void put(Object key,Object value,Long maxIdleMs,Boolean ignoreRollbackState) {
    	long startTime=System.currentTimeMillis();
    	getCache().put(this, key, value, maxIdleMs, ignoreRollbackState);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
    }
    
    @Override
    public void putAsync(Object key,Object value,Long maxIdleMs,AsyncCacheCallback<Void> callback) {
    	long startTime=System.currentTimeMillis();
    	getCache().putAsync(this, key, value, maxIdleMs, callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
    }
    
    @Override
    public Boolean putIfAbsent(Object key,Object value,Long maxIdleMs,Boolean ignoreRollbackState) {
    	long startTime=System.currentTimeMillis();
		Boolean result=getCache().putIfAbsent(this, key, value, maxIdleMs, ignoreRollbackState);  
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));
		return result;
    }
    
    @Override
    public void putIfAbsentAsync(Object key,Object value,Long maxIdleMs,AsyncCacheCallback<Boolean> callback) {
    	long startTime=System.currentTimeMillis();
		getCache().putIfAbsentAsync(this, key, value, maxIdleMs, callback);  
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
    }
    
    private InfinispanCache getCache() {
    	return localCache;
    }   
    
    public Integer getCount()
    {
    	long startTime=System.currentTimeMillis();
    	Integer result=localCache.getCount();
    	updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));
    	return result;
    }  
    
    public Set<?> getAllKeys()
    {
    	long startTime=System.currentTimeMillis();
    	Set<?> result=localCache.getAllKeys();
    	updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));
    	return result;
    }
    
    public Map<?,?> getAllElements()
    {
    	long startTime=System.currentTimeMillis();
    	Map<?,?> result=localCache.getAllElements();
    	updateStats(ClusterOperation.GET_ALL_ELEMENTS, (System.currentTimeMillis()-startTime));
    	return result;
    }

	@Override
	public TransactionManager getTransactionManager() {
		return txMgr;
	}

	@Override	
	public List<TreeSegment<?>> getAllChilds(TreeSegment<?> key,Boolean ignoreRollbackState)
	{
		long startTime=System.currentTimeMillis();
		List<TreeSegment<?>> result;
		if(key!=null)
			result=getCache().getAllChilds(this, key, ignoreRollbackState);
		else
			result=getCache().getAllChilds(this, RootTreeSegment.INSTANCE, ignoreRollbackState);
		
		updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override	
	public void getAllChildsAsync(TreeSegment<?> key,AsyncCacheCallback<List<TreeSegment<?>>> callback)
	{
		long startTime=System.currentTimeMillis();
		if(key!=null)
			getCache().getAllChildsAsync(this, key, callback);
		else
			getCache().getAllChildsAsync(this, RootTreeSegment.INSTANCE, callback);
		
		updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public Boolean hasChildrenData(TreeSegment<?> key) {
		if(key!=null)
			return getCache().hasChildren(this, key, false);
		else
			return getCache().hasChildren(this, RootTreeSegment.INSTANCE, false);	
	}

	@Override
	public void hasChildrenDataAsync(TreeSegment<?> key,AsyncCacheCallback<Boolean> callback) {
		if(key!=null)
			getCache().hasChildrenAsync(this, key, callback);
		else
			getCache().hasChildrenAsync(this, RootTreeSegment.INSTANCE, callback);	
	}
	
	@Override
	public Map<TreeSegment<?>,Object> getAllChildrenData(TreeSegment<?> key,Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		Map<TreeSegment<?>,Object> result;
		if(key!=null)
			result = getCache().getAllChildrenData(this, key, ignoreRollbackState);
		else
			result = getCache().getAllChildrenData(this, RootTreeSegment.INSTANCE, ignoreRollbackState);
		
		updateStats(ClusterOperation.GET_ALL_ELEMENTS, (System.currentTimeMillis()-startTime));
		return result;
	}
	
	@Override
	public void getAllChildrenDataAsync(TreeSegment<?> key, AsyncCacheCallback<Map<TreeSegment<?>,Object>> callback) {
		long startTime=System.currentTimeMillis();
		if(key!=null)
			getCache().getAllChildrenDataAsync(this, key, callback);
		else
			getCache().getAllChildrenDataAsync(this, RootTreeSegment.INSTANCE, callback);
		
		updateStats(ClusterOperation.GET_ALL_ELEMENTS, (System.currentTimeMillis()-startTime));
	}
	
	@Override
	public Object treeGet(TreeSegment<?> key, Boolean ignoreRollbackState)  {
		long startTime=System.currentTimeMillis();
		Object result=getCache().treeGet(this, key, ignoreRollbackState);
		updateStats(ClusterOperation.GET, (System.currentTimeMillis()-startTime));
		return result;
	}
	
	@Override
	public void treeGetAsync(TreeSegment<?> key, AsyncCacheCallback<Object> callback)  {
		long startTime=System.currentTimeMillis();
		getCache().treeGetAsync(this, key, callback);
		updateStats(ClusterOperation.GET, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public Boolean treeExists(TreeSegment<?> key, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		Boolean result=getCache().treeExists(this, key, ignoreRollbackState);
		updateStats(ClusterOperation.EXIST, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override
	public void treeExistsAsync(TreeSegment<?> key, AsyncCacheCallback<Boolean> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treeExistsAsync(this, key, callback);
		updateStats(ClusterOperation.EXIST, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public void treeRemove(TreeSegment<?> key, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		getCache().treeRemove(this, key, ignoreRollbackState, false);
		updateStats(ClusterOperation.REMOVE, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public void treeRemoveAsync(TreeSegment<?> key, AsyncCacheCallback<Void> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treeRemoveAsync(this, key, callback);
		updateStats(ClusterOperation.REMOVE, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public void treeRemoveValue(TreeSegment<?> key, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		getCache().treeRemoveValue(this, key, ignoreRollbackState, false);
		updateStats(ClusterOperation.REMOVE_VALUE, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public void treeRemoveValueAsync(TreeSegment<?> key, AsyncCacheCallback<Void> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treeRemoveValueAsync(this, key, callback);
		updateStats(ClusterOperation.REMOVE_VALUE, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public Boolean treePut(TreeSegment<?> key, Object value, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		Boolean result=getCache().treePut(this, key, value, ignoreRollbackState, false);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override
	public void treePutAsync(TreeSegment<?> key, Object value, AsyncCacheCallback<Boolean> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treePutAsync(this, key, value, callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public TreePutIfAbsentResult treePutIfAbsent(TreeSegment<?> key, Object value, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		TreePutIfAbsentResult result=getCache().treePutIfAbsent(this, key, value, ignoreRollbackState, false);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override
	public void treePutIfAbsentAsync(TreeSegment<?> key, Object value, AsyncCacheCallback<TreePutIfAbsentResult> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treePutIfAbsentAsync(this, key, value, callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
	}
	
	@Override
	public Boolean treeCreate(TreeSegment<?> key, Boolean ignoreRollbackState) {
		long startTime=System.currentTimeMillis();
		Boolean result=getCache().treeCreate(this, key, ignoreRollbackState, false);
		updateStats(ClusterOperation.CREATE, (System.currentTimeMillis()-startTime));
		return result;
	}
	
	@Override
	public void treeCreateAsync(TreeSegment<?> key, AsyncCacheCallback<Boolean> callback) {
		long startTime=System.currentTimeMillis();
		getCache().treeCreateAsync(this, key, callback);
		updateStats(ClusterOperation.CREATE, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public Boolean treeMulti(Map<TreeSegment<?>,Object> putItems,Boolean createParent,Boolean ignoreRollbackState) {
		if(putItems==null || putItems.size()==0)
			return true;
		
		Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
		TreeSegment<?> parentKey=firstEntry.getKey().getParent();
		Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<TreeSegment<?>, Object> currEntry=iterator.next();
			if(!currEntry.getKey().getParent().equals(parentKey))
				return false;
		}
		
		long startTime=System.currentTimeMillis();
		Boolean result=getCache().treeMulti(this, putItems, createParent, ignoreRollbackState);
		updateStats(ClusterOperation.COMMIT, (System.currentTimeMillis()-startTime));
		return result;
	}	

	@Override
	public void treeMultiAsync(Map<TreeSegment<?>,Object> putItems,Boolean createParent,AsyncCacheCallback<Boolean> callback) {
		if(putItems==null || putItems.size()==0) {
			callback.onSuccess(true);
			return;
		}
		
		Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
		TreeSegment<?> parentKey=firstEntry.getKey().getParent();
		Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<TreeSegment<?>, Object> currEntry=iterator.next();
			if(!currEntry.getKey().getParent().equals(parentKey)) {
				callback.onSuccess(false);
				return;
			}
		}
		
		long startTime=System.currentTimeMillis();
		getCache().treeMultiAsync(this, putItems, createParent, callback);
		updateStats(ClusterOperation.COMMIT, (System.currentTimeMillis()-startTime));		
	}	

	public void treeMarkAsPreloaded(Map<TreeSegment<?>,Object> putItems) {
		if(putItems==null || putItems.size()==0)
			return;
		
		Entry<TreeSegment<?>, Object> firstEntry=putItems.entrySet().iterator().next();
		TreeSegment<?> parentKey=firstEntry.getKey().getParent();
		Iterator<Entry<TreeSegment<?>, Object>>  iterator=putItems.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<TreeSegment<?>, Object> currEntry=iterator.next();
			if(!currEntry.getKey().getParent().equals(parentKey))
				return;
		}
		
		getCache().treeMarkAsPreloaded(this, putItems);
	}
	
	@Override
	public List<TreeSegment<?>> getChildren(TreeSegment<?> key) {
		return getAllChilds(key, false);		
	}
	
	@Override
	public void getChildrenAsync(TreeSegment<?> key,AsyncCacheCallback<List<TreeSegment<?>>> callback) {
		getAllChildsAsync(key, callback);		
	}

	@Override
	public Map<TreeSegment<?>,Object> getChildrenData(TreeSegment<?> key) {
		return getAllChildrenData(key, false);		
	}

	@Override
	public void getChildrenDataAsync(TreeSegment<?> key,AsyncCacheCallback<Map<TreeSegment<?>,Object>> callback) {
		getAllChildrenDataAsync(key, callback);		
	}

	@Override
	public void treePreload(TreeSegment<?> key) {
		getCache().preload(this, key);
	}

	@Override
	public Boolean treeIsPreloaded(TreeSegment<?> key) {
		return getCache().isPreloaded(key);
	}
	
	public void updateStats(ClusterOperation operation,long time) {
		allOperations.get(operation).incrementAndGet();
		if(time>100)
			longOperations.get(operation).incrementAndGet();
		
		operationTime.get(operation).addAndGet(time);		
	}
	
	public void viewChanged() {
		final List<String> oldView = currentView;
		List<String> newMembers=getCache().getCurrentView();
		currentView = new ArrayList<String>(newMembers);
		final String localAddress = getLocalAddress();
		
		//just a precaution, it can be null!
		if (oldView != null) {
			
			// recover stuff from lost members
			Runnable runnable = new Runnable() {
				public void run() {
					for (String oldMember : oldView) {
						if (!currentView.contains(oldMember)) {
							if (logger.isDebugEnabled()) {
								logger.debug("onViewChangeEvent : processing lost member " + oldMember);
							}
							FailOverListener listener=failOverListener.get();
							if(listener!=null) {
								List<String> electionView = currentView;
								if(electionView!=null && elector.elect(electionView).equals(localAddress))
								{
									performTakeOver(oldMember, localAddress, false);
								}
								
								//cleanAfterTakeOver(localListener, oldMember);
							}
						}
					}
				}
			};
			Thread t = new Thread(runnable);
			t.start();
		}
	}
	
	public void entryRemoved(Object key) {
		DataListener listener=dataListener.get();
		if (listener != null) {				
			listener.dataRemoved(key);
		} else {
			DataRemovalListener drListener=dataRemovalListener.get();
			if(drListener!=null) {				
				drListener.dataRemoved(key);
			}			
		}
	}
	
	public void entryCreated(Object key) {
		DataListener listener=dataListener.get();
		if (listener != null) {				
			listener.dataCreated(key);
		}
	}
	
	public void entryModified(Object key) {
		DataListener listener=dataListener.get();
		if (listener != null) {				
			listener.dataModified(key);
		}
	}
	
	@Listener
	private class ExtendedCacheListener {
		@SuppressWarnings("rawtypes")
		@CacheEntryRemoved
		public void clusterCacheEntryRemoved(CacheEntryRemovedEvent event){
			if (logger.isDebugEnabled()) {
				logger.debug("cacheEntryRemoved : event[ "+ event +"]");
			}

			if (!event.isPre() && !event.isOriginLocal() && event.getKey() != null && event.getOldValue()!=null )
				 entryRemoved(event.getKey());
		}
		
		@SuppressWarnings("rawtypes")
		@CacheEntryCreated
		public void clusterCacheEntryCreated(CacheEntryCreatedEvent event){
			if (logger.isDebugEnabled()) {
				logger.debug("cacheEntryCreated : event[ "+ event +"]");
			}

			if (!event.isPre() && !event.isOriginLocal() && event.getKey() != null )
				entryCreated(event.getKey());			
		}
		
		@SuppressWarnings("rawtypes")
		@CacheEntryModified
		public void clusterCacheEntryModified(CacheEntryModifiedEvent event){
			if (logger.isDebugEnabled()) {
				logger.debug("cacheEntryModified : event[ "+ event +"]");
			}

			if (!event.isPre() && !event.isOriginLocal() && event.getKey() != null )
				entryModified(event.getKey());			
		}
	}

	@Override
	public void clear() 
	{
		long startTime=System.currentTimeMillis();
    	localCache.clear();
    	updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));    	
	}

	@Override
	public void clearAsync(AsyncCacheCallback<Void> callback) 
	{
		long startTime=System.currentTimeMillis();
    	localCache.clearAsync(callback);
    	updateStats(ClusterOperation.GET_ALL_KEYS, (System.currentTimeMillis()-startTime));    	
	}

	@Override
	public Long getAtomicValue(String name) 
	{
		long startTime=System.currentTimeMillis();
    	Long result=getCache().getAtomicValue(name);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override
	public void getAtomicValueAsync(String name,AsyncCacheCallback<Long> callback) 
	{
		long startTime=System.currentTimeMillis();
    	getCache().getAtomicValueAsync(name,callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));		
	}

	@Override
	public Long addAndGetAtomicValue(String name, Long delta) 
	{
		long startTime=System.currentTimeMillis();
    	Long result=getCache().addAndGetAtomicValue(name, delta);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));
		return result;
	}

	@Override
	public void addAndGetAtomicValueAsync(String name, Long delta,AsyncCacheCallback<Long> callback) 
	{
		long startTime=System.currentTimeMillis();
    	getCache().addAndGetAtomicValueAsync(name, delta, callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));	
	}

	@Override
	public Boolean compareAndSetAtomicValue(String name, Long oldValue, Long newValue) 
	{
		long startTime=System.currentTimeMillis();
    	Boolean result=getCache().compareAndSetAtomicValue(name, oldValue, newValue);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));	
		return result;
	}

	@Override
	public void compareAndSetAtomicValueAsync(String name, Long oldValue, Long newValue,AsyncCacheCallback<Boolean> callback) 
	{
		long startTime=System.currentTimeMillis();
    	getCache().compareAndSetAtomicValueAsync(name, oldValue, newValue, callback);
		updateStats(ClusterOperation.PUT, (System.currentTimeMillis()-startTime));	
	}

	@Override
	public Boolean getLock(String name, Long maxWaitMS) 
	{
		return getCache().getLock(name, maxWaitMS);		
	}

	@Override
	public void getLockAsync(String name, Long maxWaitMS, AsyncCacheCallback<Boolean> callback) 
	{
		getCache().getLockAsync(name, maxWaitMS, callback);
	}

	@Override
	public void releaseLock(String name) 
	{
		getCache().releaseLock(name);
	}

	@Override
	public void releaseLockAsync(String name, AsyncCacheCallback<Void> callback) 
	{
		getCache().releaseLockAsync(name, callback);
	}
}
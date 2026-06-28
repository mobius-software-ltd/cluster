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

package org.restcomm.cluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.TransactionManager;

import org.restcomm.cluster.data.ClusterOperation;
import org.restcomm.cluster.data.TreePutIfAbsentResult;
import org.restcomm.cluster.data.TreeSegment;

/**
 * 
 * @author martins
 * @author András Kőkuti
 * @author yulian.oifa
 *
 */
public interface RestcommCluster {
	public static final String CONNECTED_CLIENT="--m--";
	
	/**
	 * Adds the specified fail over listener.
	 * @param listener
	 */
	public boolean addFailOverListener(FailOverListener listener);
	
	/**
	 * Removes the specified fail over listener.
	 * @param listener
	 * @return boolean
	 */
	public boolean removeFailOverListener(FailOverListener listener);
	
	/**
	 * Adds the specified data removal listener.
	 * @param listener
	 */
	public boolean addDataRemovalListener(DataRemovalListener listener);
	
	/**
	 * Removes the specified data removal listener.
	 * @param listener
	 * @return boolean
	 */
	public boolean removeDataRemovalListener(DataRemovalListener listener);
	
	/**
	 * Adds the specified data listener.
	 * @param listener
	 * @return boolean
	 */
	public boolean addDataListener(DataListener listener);
	
	/**
	 * Removes the specified data listener.
	 * @param listener
	 * @return boolean
	 */
	public boolean removeDataListener(DataListener listener);
	
	/**
	 * Retrieves the local address of the cluster node.
	 * @return String
	 */
	public String getLocalAddress();
	
	
	/**
     * Indicates if the cache is not in a cluster environment.
     *
     * @return boolean the localMode
     */
    public boolean isLocalMode();
    
	/**
	 * Method to determine if this node is single node in the cluster.
	 * 
	 * @return <ul>
	 *         <li><b>true</b> - cache mode is local || clusterMembers == 1
	 *         <li>
	 *         <li><b>false</b> - otherwise
	 *         <li>
	 *         </ul>
	 */
	public boolean isSingleMember();
	
	/**
	 * Starts the cluster. This should only be invoked when all listeners are
	 * added, and when all classes needed to deserialize data in a running
	 * cluster are visible (somehow).
	 */
	public void startCluster(Boolean useRemovalOnlyListener);
	
	/**
	 * Indicates if the cluster is running or not.
	 * @return boolean
	 */
	public boolean isStarted();
	
	/**
	 * Stops the cluster.
	 */
	public void stopCluster();
	
	/**
	 * Retreived cached value from cache.
	 * @return Object
	 */
	public Object treeGet(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Retreived cached value from cache.
	 */
	public void treeGetAsync(TreeSegment<?> key,AsyncCacheCallback<Object> callback);
	
	/**
	 * Validates if element exists in cache.
	 * @return Boolean
	 */
	public Boolean treeExists(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Validates if element exists in cache.
	 */
	public void treeExistsAsync(TreeSegment<?> key,AsyncCacheCallback<Boolean> callback);
	
	/**
	 * Removes the element from cache.
	 */
	public void treeRemove(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Removes the element from cache.
	 */
	public void treeRemoveAsync(TreeSegment<?> key,AsyncCacheCallback<Void> callback);
	
	/**
	 * Removes the value from cache.
	 */
	public void treeRemoveValue(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Removes the value from cache.
	 */
	public void treeRemoveValueAsync(TreeSegment<?> key,AsyncCacheCallback<Void> callback);
	
	/**
	 * Stores value to cache.
	 * @return Boolean
	 */
	public Boolean treePut(TreeSegment<?> key,Object value,Boolean ignoreRollbackState);
		
	/**
	 * Stores value to cache.
	 */
	public void treePutAsync(TreeSegment<?> key,Object value,AsyncCacheCallback<Boolean> callback);
		
	/**
	 * Stores value to cache if the parent exists and child doesnt.
	 * @return TreePutIfAbsentResult
	 */
	public TreePutIfAbsentResult treePutIfAbsent(TreeSegment<?> key,Object value,Boolean ignoreRollbackState);
		
	/**
	 * Stores value to cache if the parent exists and child doesnt.
	 */
	public void treePutIfAbsentAsync(TreeSegment<?> key,Object value,AsyncCacheCallback<TreePutIfAbsentResult> callback);
		
	/**
	 * Create a tree element.
	 * @return Boolean
	 */
	public Boolean treeCreate(TreeSegment<?> key,Boolean ignoreRollbackState);
		
	/**
	 * Create a tree element.
	 */
	public void treeCreateAsync(TreeSegment<?> key,AsyncCacheCallback<Boolean> callback);
		
	/**
	 * Multi operation to put multiple childs into tree in one op.
	 * @return Boolean
	 */
	public Boolean treeMulti(Map<TreeSegment<?>,Object> putItems,Boolean createParent,Boolean ignoreRollbackState);
	
	/**
	 * Multi operation to put multiple childs into tree in one op.
	 */
	public void treeMultiAsync(Map<TreeSegment<?>,Object> putItems,Boolean createParent, AsyncCacheCallback<Boolean> callback);
	
	/**
	 * When created out of op and then op resumed/created we may use this method to preload the data without going to cache
	 */
	public void treeMarkAsPreloaded(Map<TreeSegment<?>,Object> putItems);
	
	/**
	 * Returns all child element from cache.
	 * @return List<TreeSegment<?>>
	 */
	public List<TreeSegment<?>> getAllChilds(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Returns all child element from cache.
	 */
	public void getAllChildsAsync(TreeSegment<?> key,AsyncCacheCallback<List<TreeSegment<?>>> callback);
	
	/**
	 * Returns all child element from cache.
	 * @return List<TreeSegment<?>>
	 */
	public List<TreeSegment<?>> getChildren(TreeSegment<?> key);
	
	/**
	 * Returns all child element from cache.
	 */
	public void getChildrenAsync(TreeSegment<?> key, AsyncCacheCallback<List<TreeSegment<?>>> callback);
	
	/**
	 * Returns all values assigned for current element from cache.
	 * @return Map<TreeSegment<?>,Object>
	 */
	public Map<TreeSegment<?>,Object> getAllChildrenData(TreeSegment<?> key,Boolean ignoreRollbackState);
	
	/**
	 * Returns all values assigned for current element from cache.
	 */
	public void getAllChildrenDataAsync(TreeSegment<?> key, AsyncCacheCallback<Map<TreeSegment<?>,Object>> callback);
	
	/**
	 * Returns all values assigned for current element from cache.
	 * @return Map<TreeSegment<?>,Object>
	 */
	public Map<TreeSegment<?>,Object> getChildrenData(TreeSegment<?> key);
	
	/**
	 * Returns all values assigned for current element from cache.
	 */
	public void getChildrenDataAsync(TreeSegment<?> key,AsyncCacheCallback<Map<TreeSegment<?>,Object>> callback);
	
	/**
	 * Returns whether element has values assigned in cache.
	 * @return Boolean
	 */
	public Boolean hasChildrenData(TreeSegment<?> key);
	
	/**
	 * Returns whether element has values assigned in cache.
	 */
	public void hasChildrenDataAsync(TreeSegment<?> key,AsyncCacheCallback<Boolean> callback);
	
	/**
	 * Preloads the element children into tx cache.
	 */
	public void treePreload(TreeSegment<?> key);
	
	/**
	 * Returns whether children has been preloaded already into tx cache.
	 * @return Boolean
	 */
	public Boolean treeIsPreloaded(TreeSegment<?> key);
	
	/**
	 * Retreived cached value from cache.
	 * @return Object
	 */
	public Object get(Object key,Boolean ignoreRollbackState);
	
	/**
	 * Retreived cached value from cache.
	 */
	public void getAsync(Object key,AsyncCacheCallback<Object> callback);
	
	/**
	 * Validates if key exists in cache.
	 * @return Boolean
	 */
	public Boolean exists(Object key,Boolean ignoreRollbackState);
	
	/**
	 * Validates if key exists in cache.
	 */
	public void existsAsync(Object key,AsyncCacheCallback<Boolean> callback);
	
	/**
	 * Removes the value from cache.
	 * @return Object
	 */
	public Object remove(Object key,Boolean ignoreRollbackState,Boolean returnValue);
	
	/**
	 * Removes the value from cache.
	 */
	public void removeAsync(Object key,Boolean returnValue,AsyncCacheCallback<Object> callback);
	
	/**
	 * Stores value to cache.
	 */
	public void put(Object key,Object value, Long maxIdleMs, Boolean ignoreRollbackState);
		
	/**
	 * Stores value to cache.
	 */
	public void putAsync(Object key,Object value,Long maxIdleMs, AsyncCacheCallback<Void> callback);
		
	/**
	 * Stores value to cache if key not present.
	 * @return Boolean whether the operation succeeded
	 */
	public Boolean putIfAbsent(Object key,Object value,Long maxIdleMs, Boolean ignoreRollbackState);
		
	/**
	 * Stores value to cache if key not present.
	 */
	public void putIfAbsentAsync(Object key,Object value,Long maxIdleMs, AsyncCacheCallback<Boolean> callback);
		
	/**
	 * Returns count of elements currently in cache.
	 */
	public Integer getCount();
	
	/**
	 * Clears all elements in cache
	 */
	public void clear();
	
	/**
	 * Clears all elements in cache asynchronously
	 */
	public void clearAsync(AsyncCacheCallback<Void> callback);
	
	/**
	 * Gets atomic value
	 */
	public Long getAtomicValue(String name);
	
	/**
	 * Gets atomic value async
	 */
	public void getAtomicValueAsync(String name,AsyncCacheCallback<Long> callback);
	
	/**
	 * adds the delta to atomic value and returns new value
	 */
	public Long addAndGetAtomicValue(String name, Long delta);
	
	/**
	 * adds the delta to atomic value and returns new value async
	 */
	public void addAndGetAtomicValueAsync(String name, Long delta,AsyncCacheCallback<Long> callback);
	
	/**
	 * compares the atomic value and set the new value if old value matches
	 */
	public Boolean compareAndSetAtomicValue(String name, Long oldValue, Long newValue);
	
	/**
	 * compares the atomic value and set the new value if old value matches
	 */
	public void compareAndSetAtomicValueAsync(String name, Long oldValue, Long newValue,AsyncCacheCallback<Boolean> callback);
	
	/**
	 * tries to get distributed lock for specific period.
	 */
	public Boolean getLock(String name, Long maxWaitMS);
	
	/**
	 * tries to get distributed lock for specific period.
	 */
	public void getLockAsync(String name, Long maxWaitMS,AsyncCacheCallback<Boolean> callback);
	
	/**
	 * releases distributed lock
	 */
	public void releaseLock(String name);
	
	/**
	 * releases distributed lock async
	 */
	public void releaseLockAsync(String name,AsyncCacheCallback<Void> callback);
	
	/**
	 * Returns all keys from cache.
	 * @return Set<?>
	 */
	public Set<?> getAllKeys();
	
	/**
	 * Returns all elements from cache.
	 * @return Map<?,?>
	 */
	public Map<?,?> getAllElements();
	
	/**
	 * Returns transaction manager used with this cache.
	 * @return TransactionManager
	 */
	public TransactionManager getTransactionManager();
	
	/**
	 * update the operation statistics
	 */
	void updateStats(ClusterOperation operation,long time);
}
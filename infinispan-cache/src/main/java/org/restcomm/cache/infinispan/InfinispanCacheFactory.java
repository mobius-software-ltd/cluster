package org.restcomm.cache.infinispan;
import java.io.Externalizable;

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
import javax.transaction.TransactionManager;

import org.infinispan.manager.DefaultCacheManager;
import org.restcomm.cluster.CacheDataExecutorService;
import org.restcomm.cluster.DefaultClusterElector;
import org.restcomm.cluster.IDGenerator;
import org.restcomm.cluster.RestcommCacheFactory;
import org.restcomm.cluster.RestcommCluster;
import org.restcomm.cluster.data.ExternalizableExternalizer;
import org.restcomm.cluster.serializers.GenericJbossMarshaller;
import org.restcomm.cluster.serializers.Serializer;

/**
 * @author yulian.oifa
 */
public class InfinispanCacheFactory implements RestcommCacheFactory 
{
	private DefaultCacheManager jBossCacheContainer;
	private CacheDataExecutorService cacheExecutorService;
    private ClassLoader classLoader;
    private TransactionManager transactionManager;
	private DefaultClusterElector elector;
	private ViewChangedListener viewChangedListener;
	private IDGenerator<?> idGenerator;
	private Boolean logStats;
	
	public InfinispanCacheFactory(String clusterName, TransactionManager transactionManager,Serializer serializer,IDGenerator<?> idGenerator, ClassLoader classLoader,CacheDataExecutorService service,Integer aquireTimeout,Boolean isAsync, Boolean isReplicated, Boolean paritioned, Integer copies,Boolean logStats,Long maxIdleMs) {
		this.cacheExecutorService=service;
		this.classLoader=classLoader;
		
		this.transactionManager = transactionManager;
		this.elector = new DefaultClusterElector();
		this.idGenerator=idGenerator;
		this.logStats=logStats;
		
		final TransactionManager txMgr=transactionManager;
		if(this.jBossCacheContainer==null) {
			this.jBossCacheContainer=InfinispanCache.initContainer(clusterName, txMgr,serializer, isAsync, isReplicated, paritioned, copies, aquireTimeout, maxIdleMs);
			this.viewChangedListener=new ViewChangedListener();
			this.jBossCacheContainer.addListener(viewChangedListener);
		}
	}
	
	public void registerKnownClass(int id, Class<? extends Externalizable> knownClass) {
		this.jBossCacheContainer.getGlobalComponentRegistry().getGlobalConfiguration().serialization().advancedExternalizers().put(GenericJbossMarshaller.FIRST_ID+id,new ExternalizableExternalizer(knownClass,GenericJbossMarshaller.FIRST_ID + id));		    	
    }
	
	public void removeCluster(String name) {		
	}

	public RestcommCluster getCluster(String name,Boolean isTree) {
		if(name.equals(RestcommCluster.CONNECTED_CLIENT))
			throw new RuntimeException("The name is reserved");
		
		InfinispanCache localCache;
		if(isTree)
			localCache = new InfinispanCache(name, jBossCacheContainer, transactionManager, classLoader, cacheExecutorService, idGenerator, isTree, logStats);
		else
			localCache = new InfinispanCache(name, jBossCacheContainer, transactionManager, classLoader, cacheExecutorService, idGenerator, isTree, logStats);
		
		return new InfinispanCluster(localCache, viewChangedListener, transactionManager, elector, logStats);		
	}
	
    public void stop() {
    	if(this.jBossCacheContainer!=null) {
			this.jBossCacheContainer.stop();
			this.jBossCacheContainer=null;
		}
    	
		if(this.cacheExecutorService!=null) {
		    this.cacheExecutorService.terminate();
		    this.cacheExecutorService = null;
		}
    }
    
    public DefaultCacheManager getCacheManager()
    {
    	return jBossCacheContainer;
    }
}

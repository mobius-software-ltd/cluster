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

package org.restcomm.cluster;


/**
 * Different cluster wide unique identifiers types
 * @author yulian.oifa
 *
 */
public enum ClusterType {

	/**
	 * INFINISPAN LOCAL CLUSTER
     */
	INFINISPAN_LOCAL,
	/**
	 * INFINISPAN REPLICATED CLUSTER
     */
	INFINISPAN_REPLICATED,
	/**
	 * INFINISPAN WITH ZK BASED TREEE REPLICATED CLUSTER
     */
	INFINISPAN_ZK_REPLICATED,
	/**
	 * HAZELCAST WITH ZK BASED TREEE REPLICATED CLUSTER
     */
	HAZELCAST_ZK_REPLICATED,
	/**
	 * HAZELCAST WITH INFINISPAN BASED TREEE REPLICATED CLUSTER
     */
	HAZELCAST_IN_REPLICATED,
	/**
	 * IGNITE WITH ZK BASED TREEE REPLICATED CLUSTER
     */
	IGNITE_ZK_REPLICATED,
	/**
	 * IGNITE WITH INFINISPAN BASED TREEE REPLICATED CLUSTER
     */
	IGNITE_IN_REPLICATED
}

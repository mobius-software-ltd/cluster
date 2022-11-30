package org.restcomm.cluster.data;
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

import net.jcip.annotations.Immutable;

/**
 * 
 * string based tree data key segment
 * 
 * @author yulian.oifa
 *
 */
@Immutable
public class StringTreeSegment extends AbstractTreeSegment<String>
{
	private static final long serialVersionUID = 1L;

	public StringTreeSegment(String value,TreeSegment<?> parent) {
		super(value, parent);
	}
	
	public StringTreeSegment(Object[] list) {
		super(list);
	}

	@Override
	public Boolean isRoot() {
		return false;
	}
}
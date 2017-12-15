/*
 * Copyright 2017 Olov McKie
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.postgres;

import java.sql.Connection;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import se.uu.ub.cora.sqlstorage.SqlStorageException;

public class ContextConnectionProviderImp implements SqlConnectionProvider {
	private InitialContext context;
	private String name;
	private DataSource ds;

	public String getName() {
		// for test
		return name;
	}

	public InitialContext getContext() {
		// for test
		return context;
	}

	public static ContextConnectionProviderImp usingInitialContextAndName(InitialContext context,
			String name) {
		return new ContextConnectionProviderImp(context, name);
	}

	private ContextConnectionProviderImp(InitialContext context, String name) {
		this.context = context;
		this.name = name;
	}

	@Override
	public Connection getConnection() {
		try {
			lookupDatasourceUsingNameIfNotLookedUpSinceBefore(context, name);
			return ds.getConnection();
		} catch (Exception e) {
			throw SqlStorageException.withMessage(e.getMessage());
		}
	}

	private void lookupDatasourceUsingNameIfNotLookedUpSinceBefore(InitialContext context,
			String name) throws NamingException {
		if (null == ds) {
			ds = (DataSource) context.lookup(name);
		}
		if (ds == null) {
			throw SqlStorageException.withMessage("Data source not found!");
		}
	}
}

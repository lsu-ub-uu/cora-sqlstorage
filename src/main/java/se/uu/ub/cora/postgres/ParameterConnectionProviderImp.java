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

public class ParameterConnectionProviderImp implements SqlConnectionProvider {

	private DataSource ds;

	public static ParameterConnectionProviderImp usingInitialContext(InitialContext context,
			String name) {
		return new ParameterConnectionProviderImp(context, name);
	}

	public static ParameterConnectionProviderImp usingUriAndUserAndPassword(String uri, String user,
			String password) {
		return new ParameterConnectionProviderImp(uri, user, password);
	}

	private ParameterConnectionProviderImp(InitialContext context, String name) {
		try {
			lookupDatasourceUsingName(context, name);
		} catch (Exception e) {
			throw SqlStorageException.withMessage(e.getMessage());
		}
	}

	private void lookupDatasourceUsingName(InitialContext context, String name)
			throws NamingException {
		ds = (DataSource) context.lookup(name);
		if (ds == null) {
			throw SqlStorageException.withMessage("Data source not found!");
		}
	}

	private ParameterConnectionProviderImp(String uri, String user, String password) {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see se.uu.ub.cora.postgres.SqlConnectionProvider#getConnection()
	 */
	@Override
	public Connection getConnection() {
		// try {
		// Connection c =
		// DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb",
		// "postgres", "123");
		// DriverManager.
		// } catch (Exception e) {
		// throw SqlStorageException.withMessage(e.getMessage());
		// e.printStackTrace();
		// System.err.println(e.getClass().getName() + ": " + e.getMessage());
		// System.exit(0);
		// }
		// System.out.println("Opened database successfully");
		try {
			return ds.getConnection();
		} catch (Exception e) {
			throw SqlStorageException.withMessage(e.getMessage());
		}
	}

}

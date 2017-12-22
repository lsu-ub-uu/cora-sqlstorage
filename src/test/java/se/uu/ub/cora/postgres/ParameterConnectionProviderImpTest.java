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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.sql.Connection;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParameterConnectionProviderImpTest {

	// Connection c = null;
	// try {
	// Class.forName("org.postgresql.Driver");
	// c = DriverManager
	// .getConnection("jdbc:postgresql://localhost:5432/testdb",
	// "postgres", "123");
	// } catch (Exception e) {
	// e.printStackTrace();
	// System.err.println(e.getClass().getName()+": "+e.getMessage());
	// System.exit(0);
	// }
	// System.out.println("Opened database successfully");

	private SqlConnectionProvider postgresConnectionProviderImp;
	private InitialContextSpy contextSpy;
	private String name = "java:/comp/env/jdbc/postgres";

	private String uri = "jdbc:postgresql://localhost:5432/testdb";
	private String user = "";
	private String password = "";

	@BeforeMethod
	public void setUp() throws Exception {
		contextSpy = new InitialContextSpy();
		// postgresConnectionProviderImp = ContextConnectionProviderImp
		// .usingUriAndUserAndPassword(uri, user, password);
		postgresConnectionProviderImp = ContextConnectionProviderImp
				.usingInitialContextAndName(contextSpy, name);
	}

	// @Test(expectedExceptions = SqlStorageException.class)
	// public void testInitProblemWithNullDataSource() throws Exception {
	// InitialContextSpy context = new InitialContextSpy();
	// context.ds = null;
	// postgresConnectionProviderImp =
	// ContextConnectionProviderImp.usingInitialContextAndName(context,
	// name);
	// assertNotNull(postgresConnectionProviderImp);
	// }
	//
	// @Test
	// public void testNameIsReadFromInitialContext() {
	// assertNotNull(postgresConnectionProviderImp);
	// assertEquals(contextSpy.name, name);
	// }

	@Test
	public void testGetConnectionIsFetchedFromDatasource() throws Exception {
		Connection con = postgresConnectionProviderImp.getConnection();
		assertNotNull(con);
		DataSourceSpy dsSpy = (DataSourceSpy) contextSpy.ds;
		assertEquals(con, dsSpy.connectionList.get(0));
	}
}

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

import se.uu.ub.cora.sqlstorage.SqlStorageException;

public class ContextConnectionProviderImpTest {

	private ContextConnectionProviderImp contextConnectionProviderImp;
	private InitialContextSpy contextSpy;
	private String name = "java:/comp/env/jdbc/postgres";

	@BeforeMethod
	public void setUp() throws Exception {
		contextSpy = new InitialContextSpy();
		contextConnectionProviderImp = ContextConnectionProviderImp
				.usingInitialContextAndName(contextSpy, name);
	}

	@Test
	public void testContextAndNameAreSet() {
		assertEquals(contextConnectionProviderImp.getContext(), contextSpy);
		assertEquals(contextConnectionProviderImp.getName(), name);
	}

	@Test
	public void testNameIsReadFromInitialContextOnlyOnFirstGetConnection() {
		assertNotNull(contextConnectionProviderImp);
		assertEquals(contextSpy.noOfLookups, 0);
		contextConnectionProviderImp.getConnection();
		assertEquals(contextSpy.noOfLookups, 1);
		assertEquals(contextSpy.name, name);
		contextConnectionProviderImp.getConnection();
		assertEquals(contextSpy.noOfLookups, 1);
	}

	@Test
	public void testGetConnectionIsFetchedFromDatasource() throws Exception {
		Connection con = contextConnectionProviderImp.getConnection();
		assertNotNull(con);
		DataSourceSpy dsSpy = (DataSourceSpy) contextSpy.ds;
		assertEquals(con, dsSpy.connectionList.get(0));
	}

	@Test(expectedExceptions = SqlStorageException.class)
	public void testInitProblemWithNullDataSource() throws Exception {
		// InitialContextSpy context = new InitialContextSpy();
		// context.ds = null;
		// contextConnectionProviderImp =
		// ContextConnectionProviderImp.usingInitialContextAndName(context,
		// name);
		// assertNotNull(contextConnectionProviderImp);
		contextSpy.ds = null;
		contextConnectionProviderImp.getConnection();
	}
}

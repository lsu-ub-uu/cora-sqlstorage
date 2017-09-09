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

import static org.testng.Assert.assertNotNull;

import java.sql.Connection;

import javax.sql.DataSource;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.postgres.PostgresConnectionProviderImp;
import se.uu.ub.cora.postgres.SqlStorageException;

public class PostgresConnectionProviderImpTest {

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

	private PostgresConnectionProviderImp postgresConnectionProviderImp;

	@BeforeMethod
	public void setUp() {
		String uri = "jdbc:postgresql://localhost:5432/testdb";
		String user = "";
		String password = "";
		DataSource d;

		// TODO: send in the initialContext
		postgresConnectionProviderImp = PostgresConnectionProviderImp
				.usingUriAndUserAndPassword(uri, user, password);

	}

	@Test
	public void testInit() {
		assertNotNull(postgresConnectionProviderImp);
	}

	@Test(expectedExceptions = SqlStorageException.class)
	public void testGetConnection() {
		Connection con = postgresConnectionProviderImp.getConnection();
		// assertNotNull(con);
	}
}

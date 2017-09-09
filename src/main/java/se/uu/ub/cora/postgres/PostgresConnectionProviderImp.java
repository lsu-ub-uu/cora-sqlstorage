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
import javax.sql.DataSource;

public class PostgresConnectionProviderImp {

	private PostgresConnectionProviderImp(String uri, String user, String password) {
		// TODO Auto-generated constructor stub
	}

	public static PostgresConnectionProviderImp usingUriAndUserAndPassword(String uri, String user,
			String password) {
		return new PostgresConnectionProviderImp(uri, user, password);
	}

	public Connection getConnection() {
		Connection c = null;
		// try {
		// c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb",
		// "postgres",
		// "123");
		// } catch (Exception e) {
		// throw SqlStorageException.withMessage(e.getMessage());
		// // e.printStackTrace();
		// // System.err.println(e.getClass().getName() + ": " + e.getMessage());
		// // System.exit(0);
		// }
		// System.out.println("Opened database successfully");

		try {
			InitialContext cxt = new InitialContext();
			DataSource ds = (DataSource) cxt.lookup("java:/comp/env/jdbc/postgres");

			if (ds == null) {
				throw new Exception("Data source not found!");
			}
			System.out.println("Opened database successfully");
			return c;
		} catch (Exception e) {
			throw SqlStorageException.withMessage(e.getMessage());
		}
	}

}

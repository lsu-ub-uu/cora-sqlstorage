package se.uu.ub.cora.postgres;

import java.sql.Connection;

public interface SqlConnectionProvider {

	Connection getConnection();

}
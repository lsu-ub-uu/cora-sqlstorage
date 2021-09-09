module se.uu.ub.cora.sqlstorage {
	requires transitive java.sql;
	requires se.uu.ub.cora.storage;
	requires transitive se.uu.ub.cora.sqldatabase;

	exports se.uu.ub.cora.sqlstorage;
}
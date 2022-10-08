import se.uu.ub.cora.sqlstorage.DatabaseStorageInstanceProvider;

/**
 * The sqlstorage module provides interfaces and access needed to use a sql database as storage in a
 * Cora based system.
 * <p>
 * 
 * @provides se.uu.ub.cora.storage.RecordStorageProvider
 */
module se.uu.ub.cora.sqlstorage {
	requires se.uu.ub.cora.storage;
	requires se.uu.ub.cora.sqldatabase;
	requires se.uu.ub.cora.logger;
	requires org.postgresql.jdbc;
	requires java.sql;
	requires se.uu.ub.cora.initialize;

	// Temporal export. Should be removed when DatabaseStorageProvider can be load via a service
	// loader.
	// exports se.uu.ub.cora.sqlstorage;

	provides se.uu.ub.cora.storage.RecordStorageInstanceProvider with DatabaseStorageInstanceProvider;
}
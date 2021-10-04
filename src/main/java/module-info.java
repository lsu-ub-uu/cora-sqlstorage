import se.uu.ub.cora.sqlstorage.DatabaseStorageProvider;

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

	provides se.uu.ub.cora.storage.RecordStorageProvider with DatabaseStorageProvider;
}
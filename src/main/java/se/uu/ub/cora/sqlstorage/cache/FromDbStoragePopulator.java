package se.uu.ub.cora.sqlstorage.cache;

import se.uu.ub.cora.storage.RecordStorage;

public interface FromDbStoragePopulator {

	void populateStorageFromDatabase(RecordStorage recordStorageInMemory);

}
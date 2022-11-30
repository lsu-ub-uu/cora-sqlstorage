package se.uu.ub.cora.sqlstorage.cache;

import se.uu.ub.cora.storage.RecordStorage;
import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;

public class FromDbStoragePopulatorSpy implements FromDbStoragePopulator {

	public MethodCallRecorder MCR = new MethodCallRecorder();

	@Override
	public void populateStorageFromDatabase(RecordStorage recordStorageInMemory) {
		MCR.addCall("recordStorageInMemory", recordStorageInMemory);

	}

}

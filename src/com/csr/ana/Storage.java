package com.csr.ana;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.fleety.util.pool.timer.FleetyTimerTask;
import com.labServer.manager.LabDisplayParamterManager;
import com.labServer.manager.LabDisplayParamterManagerImpl;
import com.labServer.manager.LabInputParamterManager;
import com.labServer.manager.LabInputParamterManagerImpl;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabInputParamter;

public class Storage extends FleetyTimerTask{

	private BlockingQueue<LabDisplayParamter> displayQueue;
	private BlockingQueue<LabInputParamter> inputQueue;
	private LabInputParamterManager labInputParamterManager = new LabInputParamterManagerImpl();
	private LabDisplayParamterManager labDisplayParamterManager = new LabDisplayParamterManagerImpl();

	public Storage(BlockingQueue<LabDisplayParamter> displayQueue, BlockingQueue<LabInputParamter> inputQueue) {
		this.displayQueue = displayQueue;
		this.inputQueue = inputQueue;
	}

	@Override
	public void run() {
		System.out.println("Storage...");
		while (true) {
			if (inputQueue.size() != 0) {
				List<LabInputParamter> listInputItems = new ArrayList<>();
				List<LabDisplayParamter> listDisplayItems = new ArrayList<>();
				inputQueue.drainTo(listInputItems);
				displayQueue.drainTo(listDisplayItems);
				labDisplayParamterManager.addListItemsToDiffDisplay(listDisplayItems);
				labInputParamterManager.addListItemsToSumInput(listInputItems);
				labDisplayParamterManager.addListItemsToSumDisplay(listDisplayItems);
				listDisplayItems.clear();
				listInputItems.clear();
			}
		}
	}
}

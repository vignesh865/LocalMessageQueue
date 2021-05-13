package com.wizenoze.assignment.messagequeue;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractScheduledService;

public class MessageTimeoutMonitor extends AbstractScheduledService {

	public static final int LOCK_FILE_SIZE = 1024 * 10;

	public static final String LOCK_FILE = ".monitorlock";
	public static final int MESSAGE_PROCESSING_TIMEOUT = 1;

	private final int processingTimeout;

	Map<String, Integer> lastScannedPositionMap = new HashMap<>();

	private MessageTimeoutMonitor(int processingTimeOut) {
		super();
		this.processingTimeout = processingTimeOut;
	}

	public static void setReferenceTime(String queueName) throws IOException {

		FileQueue queue = new FileQueue(LOCK_FILE, LOCK_FILE_SIZE);
		FileLock lock = queue.getLock();

		queue.writeString(String.valueOf(System.currentTimeMillis()), 0);

		lock.release();
		queue.destroy();
	}

	public static void registerQueue(String queueName) throws IOException {

		FileQueue queue = new FileQueue(LOCK_FILE, LOCK_FILE_SIZE);
		FileLock lock = queue.getLock();

		queue.writeString(queueName);

		lock.release();
		queue.destroy();
	}

	@Override
	protected void runOneIteration() throws Exception {
		Set<String> queueNames = getAllQueues();

		if (queueNames.isEmpty()) {
			System.out.println("No queues registered for monitoring");
		}

	}

	private static Set<String> getAllQueues() throws IOException {

		FileQueue queue = new FileQueue(LOCK_FILE);
		FileLock lock = queue.getLock();

		Set<String> queueNames = new HashSet<>();

		while (true) {
			try {
				queueNames.add(queue.fetchString());
			} catch (EndOfDataException exception) {
				break;
			}
		}

		lock.release();
		queue.destroy();

		return queueNames;
	}


	public void startMonitor() {

		File file = new File(LOCK_FILE);

		if (file.exists()) {
			System.out.println("Monitor already present. Cannot start new monitor");
			return;
		}

		startAsync();
	}

	@Override
	protected Scheduler scheduler() {
		/*
		 * Starting monitor after initial delay of processingTimeOut and frequency of
		 * execution set to processingTimeout.
		 */
		return Scheduler.newFixedRateSchedule(processingTimeout, processingTimeout, TimeUnit.SECONDS);
	}

}

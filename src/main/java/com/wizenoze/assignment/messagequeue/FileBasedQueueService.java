package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public class FileBasedQueueService implements QueueService {

	private final FileQueue queue;
	private final FileQueue pushStatus;

	private static final int PULL_POSITION_META_START_BIT = 0;
	public static final int PUSH_POSITION_META_START_BIT = 32;
	private static final int INT_BIT_LENGTH = 32;

	public FileBasedQueueService(String queueName) throws IOException {
		this.queue = new FileQueue(queueName);
		this.pushStatus = new FileQueue(queueName + "-pushStatus", 1);
		setInitialBits();
	}

	private void setInitialBits() {
		try {
			// Ignore if it is not a new file
			this.queue.fetchInt();
		} catch (EndOfDataException exception) {
			this.queue.writeInt(64, PUSH_POSITION_META_START_BIT);
			this.queue.writeInt(64, PULL_POSITION_META_START_BIT);
			this.pushStatus.writeBool(false, 0);
		}
	}

	public String getQueueName() {
		return queue.getQueueName();
	}

	@Override
	public synchronized boolean push(String message) throws IOException {
		FileLock lock = null;
		try {
			lock = queue.getLock();

			int currentPosition = queue.fetchInt(PUSH_POSITION_META_START_BIT);
			queue.writeString(message, currentPosition);
			queue.writeInt(currentPosition + message.length() + INT_BIT_LENGTH, PUSH_POSITION_META_START_BIT);

			return true;
		} catch (OverlappingFileLockException exception) {
			return false;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}
	}

	@Override
	public synchronized String pull() throws IOException {
		FileLock lock = null;

		try {
			lock = queue.getLock();

			final int currentPosition = queue.fetchInt(PULL_POSITION_META_START_BIT);
			final String message = queue.fetchString(currentPosition);
			queue.writeInt(currentPosition + message.length() + INT_BIT_LENGTH, PULL_POSITION_META_START_BIT);

			return message;
		} catch (EndOfDataException | OverlappingFileLockException exception) {
			return null;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}
	}

	@Override
	public boolean hasAllMessagesConsumed() throws IOException {
		boolean pushEndMarked = pushStatus.fetchBool(0);

		if (!pushEndMarked) {
			return false;
		}

		return queue.fetchInt(PULL_POSITION_META_START_BIT) == queue.fetchInt(PUSH_POSITION_META_START_BIT);
	}

	@Override
	public void shutdown() throws IOException {
		this.queue.destroy();
	}

	@Override
	public void delete(int messageId) {
		// TODO Auto-generated method stub

	}

}

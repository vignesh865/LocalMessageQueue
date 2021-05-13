package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public class FileBasedQueueService implements QueueService {

	private final FileQueue queue;
	private final FileQueue pushStatus;

	private static final int PULL_POSITION_META_START_BIT = 0;
	public static final int PUSH_POSITION_META_START_BIT = 32;
	public static final int INT_BIT_LENGTH = 32;
	public static final int SHORT_INT_BIT_LENGTH = 4;
	public static final int DATA_START_INDEX = 64;

	public static final int INVALID_POSITON = -1;

	public FileBasedQueueService(String queueName, int size) throws IOException {
		this.queue = new FileQueue(queueName, size);
		this.pushStatus = new FileQueue(queueName + "-pushStatus", 1);
		setInitialBits();
	}

	private void setInitialBits() {
		try {
			// Ignore if it is not a new file
			this.queue.fetchInt();
		} catch (EndOfDataException exception) {
			this.queue.writeInt(DATA_START_INDEX, PUSH_POSITION_META_START_BIT);
			this.queue.writeInt(DATA_START_INDEX, PULL_POSITION_META_START_BIT);
			this.pushStatus.writeBool(false, 0);
		}
	}

	public String getQueueName() {
		return queue.getQueueName();
	}

	@Override
	public synchronized int push(String message) throws IOException {
		FileLock lock = null;
		try {
			lock = queue.getLock();

			int currentPosition = queue.fetchInt(PUSH_POSITION_META_START_BIT);
			queue.writeString(message, currentPosition, MessageStatus.UNPROCESSED);
			queue.writeInt(incrementedPosition(currentPosition, message.length()), PUSH_POSITION_META_START_BIT);

			return currentPosition;
		} catch (OverlappingFileLockException exception) {
			return INVALID_POSITON;
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
			queue.writeInt(incrementedPosition(currentPosition, message.length()), PULL_POSITION_META_START_BIT);

			int statusPosition = statusPositionInMessage(currentPosition, message.length());
			if (queue.fetchShortInt(statusPosition) == MessageStatus.DELETED.status) {
				return null;
			}

			queue.writeShortInt(MessageStatus.IN_PROCESS.status, statusPosition);
			lock.release();

			/*
			 * Set the message status to IN_PROCESS. Release the file lock and send the
			 * pulled message for processing.
			 */
			processMessage(message);

			lock = queue.getLock();
			queue.writeShortInt(MessageStatus.PROCESSED.status, statusPosition);
			return message;
		} catch (EndOfDataException | OverlappingFileLockException exception) {
			return null;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}
	}

	private static int incrementedPosition(int currentPosition, int messageLength) {
		return currentPosition + messageLength + INT_BIT_LENGTH + SHORT_INT_BIT_LENGTH;
	}

	private static int statusPositionInMessage(int currentPosition, int messageLength) {
		return currentPosition + messageLength + INT_BIT_LENGTH;
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
	public void delete(int messageIndex) {

		final String message = queue.fetchString(messageIndex);
		int statusPosition = statusPositionInMessage(messageIndex, message.length());

		int status = queue.fetchShortInt(statusPosition);

		if (status == MessageStatus.PROCESSED.status) {
			throw new IllegalArgumentException("Message delivered already");
		}

		queue.writeShortInt(MessageStatus.DELETED.status, statusPosition);

	}

}

package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.TimeoutException;

public interface QueueService {

	int push(String message) throws IOException, OverlappingFileLockException;

	String pull() throws IOException;

	void delete(int messageId);

	String getQueueName();

	boolean hasAllMessagesConsumed() throws IOException;

	void shutdown() throws IOException;

	default boolean processMessage(String message) throws IOException, TimeoutException {
		System.out.println(message);
		return true;
	}

	public static String getPushStatusQueueName(String queueName) {
		return queueName + "-pushStatus";
	}

	public static String getDLQName(String queueName) {
		return queueName + "-dlq";
	}

}

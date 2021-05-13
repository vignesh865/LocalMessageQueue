package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface QueueService {

	int push(String message) throws IOException;

	int pushWithRetry(String message) throws IOException;

	String pull() throws IOException;

	void delete(int messageId);

	boolean hasAllMessagesConsumed() throws IOException;

	void shutdown() throws IOException;

	default boolean processMessage(String message) throws IOException, TimeoutException {
		System.out.println(message);
		return true;
	}

}

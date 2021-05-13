package com.wizenoze.assignment.messagequeue;

import java.io.IOException;

public interface QueueService {

	int push(String message) throws IOException;

	String pull() throws IOException;

	void delete(int messageId);

	boolean hasAllMessagesConsumed() throws IOException;

	void shutdown() throws IOException;

	default void processMessage(String message) throws IOException {
		System.out.println(message + "***");
	}

}

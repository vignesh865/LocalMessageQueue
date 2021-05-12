package com.wizenoze.assignment.messagequeue;

import java.io.IOException;

public interface QueueService {

	boolean push(String message) throws IOException;

	String pull() throws IOException;

	void delete(int messageId);

	boolean hasAllMessagesConsumed() throws IOException;

	void shutdown() throws IOException;

}

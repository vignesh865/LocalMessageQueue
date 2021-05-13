package com.wizenoze.assignment.messagequeue;

public enum MessageStatus {

	UNPROCESSED(0), IN_PROCESS(1), PROCESSED(2), DELETED(3);

	int status;

	MessageStatus(int status) {
		this.status = status;
	}
}

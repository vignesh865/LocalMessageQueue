package com.wizenoze.assignment.messagequeue;

public enum MessageStatus {

	UNPROCESSED(0), PROCESSED(1), DELETED(2);

	int status;

	MessageStatus(int status) {
		this.status = status;
	}
}

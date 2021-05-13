package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class FileQueue {

	private final String queueName;
	private final RandomAccessFile file;
	private final FileChannel channel;
	private final MappedByteBuffer datasource;

	// 500 mb 524288000 bytes
	public static final int DEFAULT_STORAGE_SIZE = 524288000;
	private static final String EXTENSION = ".queue";

	public FileQueue(String queueName) throws IOException {
		this.queueName = queueName;
		this.file = new RandomAccessFile(String.format("%s%s", queueName, EXTENSION), "rw");
		this.channel = this.file.getChannel();
		this.datasource = channel.map(FileChannel.MapMode.READ_WRITE, 0, DEFAULT_STORAGE_SIZE);
	}

	public FileQueue(String queueName, int storageSize) throws IOException {
		this.queueName = queueName;
		this.file = new RandomAccessFile(String.format("%s%s", queueName, EXTENSION), "rw");
		this.channel = this.file.getChannel();
		this.datasource = channel.map(FileChannel.MapMode.READ_WRITE, 0, storageSize);
	}

	public String getQueueName() {
		return queueName;
	}

	public FileLock getLock() throws IOException {
		return channel.lock();
	}

	public FileLock getLock(int at, int size) throws IOException {
		return channel.lock(at, size, false);
	}

	public MappedByteBuffer getDatasource() {
		return datasource;
	}

	public void setPosition(int position) {
		datasource.position(position);
	}

	public void writeString(String message) {
		writeInt(message.length());
		datasource.put(message.getBytes());
	}

	public void writeString(String message, int at) {
		setPosition(at);
		writeString(message);
	}

	public void writeString(String message, MessageStatus status) {
		writeInt(message.length());
		datasource.put(message.getBytes());
		writeShortInt(status.status);
	}

	public void writeString(String message, int at, MessageStatus status) {
		setPosition(at);
		writeString(message, status);
	}

	public void writeBool(boolean flag, int at) {
		datasource.put(at, CommonUtils.toByte(flag));
	}

	public boolean fetchBool() {
		return CommonUtils.nextBool(datasource);
	}

	public boolean fetchBool(int at) {
		setPosition(at);
		return CommonUtils.nextBool(datasource);
	}

	public void writeInt(int value) {
		datasource.put(CommonUtils.toBinaryString(value).getBytes());
	}

	public void writeShortInt(int value) {
		datasource.put(CommonUtils.toShortString(value).getBytes());
	}

	public void writeShortInt(int value, int at) {
		setPosition(at);
		datasource.put(CommonUtils.toShortString(value).getBytes());
	}

	public void writeInt(int value, int at) {
		setPosition(at);
		writeInt(value);
	}

	public String fetchString() {
		int length = CommonUtils.nextInt(datasource);
		return CommonUtils.nextString(datasource, length);
	}

	public int fetchInt() {
		return CommonUtils.nextInt(datasource);
	}

	public String fetchString(int at) {
		setPosition(at);
		return fetchString();
	}

	public int fetchInt(int at) {
		setPosition(at);
		return fetchInt();
	}

	public int fetchShortInt(int at) {
		setPosition(at);
		return CommonUtils.nextShortInt(datasource);
	}

	public void destroy() throws IOException {
		this.datasource.clear();
		this.channel.close();
		this.file.close();
	}

}

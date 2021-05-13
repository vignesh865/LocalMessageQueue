package com.wizenoze.assignment.messagequeue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class CommonUtils {

	public static String toBinaryString(int value) {
		return StringUtils.leftPad(Integer.toBinaryString(value), FileBasedQueueService.INT_BIT_LENGTH, '0');
	}

	public static String toShortString(int value) {
		return StringUtils.leftPad(Integer.toBinaryString(value), FileBasedQueueService.SHORT_INT_BIT_LENGTH, '0');
	}

	public static int nextInt(MappedByteBuffer out, int at) {
		String str = nextString(out, FileBasedQueueService.INT_BIT_LENGTH, at);
		if (StringUtils.isEmpty(str.trim())) {
			throw new EndOfDataException();
		}
		return Integer.parseInt(str, 2);
	}

	public static String nextString(MappedByteBuffer out, int length, int at) {
		byte[] word = new byte[length];
		for (int i = at, j = 0; i <= at + length; i++, j++) {
			word[j] = out.get(i);
		}
		String str = new String(word, StandardCharsets.UTF_8);
		return str;
	}

	public static int nextInt(MappedByteBuffer out) {
		String str = nextString(out, FileBasedQueueService.INT_BIT_LENGTH);
		if (StringUtils.isEmpty(str.trim())) {
			throw new EndOfDataException();
		}
		return Integer.parseInt(str, 2);
	}

	public static int nextShortInt(MappedByteBuffer out) {
		String str = nextString(out, FileBasedQueueService.SHORT_INT_BIT_LENGTH);
		if (StringUtils.isEmpty(str.trim())) {
			throw new EndOfDataException();
		}
		return Integer.parseInt(str, 2);
	}

	public static String nextString(MappedByteBuffer out, int length) {
		byte[] word = new byte[length];
		out.get(word, 0, length);
		String str = new String(word, StandardCharsets.UTF_8);
		return str;
	}

	public static byte toByte(boolean vIn) {
		return (byte) (vIn ? 1 : 0);
	}

	public static boolean nextBool(MappedByteBuffer out) {
		return out.get() != 0;
	}

	public static void markPushEnd(String queueName) throws IOException {
		FileQueue queue = new FileQueue(QueueService.getPushStatusQueueName(queueName), 1);
		markPushEnd(queue);
		queue.destroy();
	}

	private static void markPushEnd(FileQueue pushStatusQueue) throws IOException {
		FileLock lock = null;
		try {
			lock = pushStatusQueue.getLock();
			pushStatusQueue.writeBool(true, 0);

		} catch (OverlappingFileLockException exception) {
			return;
		} finally {
			if (lock != null) {
				lock.release();
			}
		}
	}

	public static List<File> getFiles(String dirName, String extension) {
		File dir = new File(dirName);

		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				return filename.endsWith(extension);
			}
		});

		return Arrays.asList(files);
	}

	public static void deleteAllFiles(String dirName, String extension) {

		List<File> files = getFiles(dirName, FileQueue.EXTENSION);

		files.forEach(file -> {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				System.out.println("Problem with deleting queue file - " + file.getAbsolutePath());
			}
		});
	}
}

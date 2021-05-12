package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.FileLock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerExecutor {

	// testing2303665 100mb
	// testing4583178 200mb
	// 1,14,45,873 500mb

	public static void main(String[] args) throws Exception {
		execute(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	}

	public static void execute(String topic, int messageCount, int producercount)
			throws IOException, InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(producercount);

		int currentProducer = 0;
		while (currentProducer < producercount) {
			executor.execute(new Producer("producer" + currentProducer, topic, messageCount));
			currentProducer++;
		}

		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		markPushEnd(topic);
	}

	private static void markPushEnd(String queueName) throws IOException {
		FileQueue pushStatusQueue = new FileQueue(queueName + "-pushStatus", 1);
		FileLock lock = pushStatusQueue.getLock();
		pushStatusQueue.writeBool(true, 0);
		lock.release();
	}

	private static class Producer implements Runnable {

		final String producerId;
		final QueueService queue;
		final int messageCount;

		Producer(String producerId, String topic, int messageCount) throws IOException {
			this.producerId = producerId;
			this.queue = new FileBasedQueueService(topic);
			this.messageCount = messageCount;
		}

		@Override
		public void run() {
			try {
				produce(producerId, queue, messageCount);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}

		}

		private static void produce(String producerId, QueueService testQueue, int messageCount)
				throws IOException, InterruptedException {

			int currentMessageCount = 0;
			while (currentMessageCount <= messageCount) {
				String msg = String.format("%s-%s", producerId, currentMessageCount);

				try {

					boolean isSuccessful = testQueue.push(msg);
					if (isSuccessful) {
						currentMessageCount++;
					}

				} catch (BufferOverflowException exception) {
					System.out.println("Memory limit exceeded. Total messages pushed - " + currentMessageCount);
					break;
				}

			}

			System.out.println(String.format("Total %s messages pushed by %s", currentMessageCount, producerId));
		}
	}

}

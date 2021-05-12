package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerExecutor {

	public static void main(String[] args) throws Exception {
		execute(args[0], Integer.parseInt(args[1]));
	}

	public static void execute(String topic, int consumerCount) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(consumerCount);

		int currentConsumer = 0;
		while (currentConsumer < consumerCount) {
			executor.execute(new Consumer("consumer" + currentConsumer, topic));
			currentConsumer++;
		}

		executor.shutdown();
	}

	private static class Consumer implements Runnable {

		final String consumerId;
		final QueueService queue;

		Consumer(String consumerId, String topic) throws IOException {
			this.consumerId = consumerId;
			this.queue = new FileBasedQueueService(topic);
		}

		@Override
		public void run() {
			try {
				consume(consumerId, queue);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		public static void consume(String consumerId, QueueService testQueue) throws Exception {

			int currentMessageCount = 0;
			while (!testQueue.hasAllMessagesConsumed()) {
				String message = null;

				message = testQueue.pull();

				if (message != null) {
					currentMessageCount++;
					System.out.println(message);
				}

			}

			System.out.println(String.format("Total %s messages consumed by %s", currentMessageCount, consumerId));
		}

	}

}

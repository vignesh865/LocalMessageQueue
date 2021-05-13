package com.wizenoze.assignment.messagequeue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 * 
 *  Message count upper limit for 100 mb: ~2303665 
 *  Message count upper limit for 200 mb: ~4583178 
 *  Message count upper limit for 500 mb: ~11445873

 */
public class ProducerExecutor {

	private int queueSize = FileQueue.DEFAULT_STORAGE_SIZE;

	public static void main(String[] args) throws Exception {
		ProducerExecutor producer = new ProducerExecutor();
		producer.execute(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	}

	public synchronized void execute(String topic, int messageCount, int producercount) throws IOException, InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(producercount);

		int currentProducer = 0;
		while (currentProducer < producercount) {
			executor.execute(new Producer("producer" + currentProducer, topic, messageCount));
			currentProducer++;
		}

		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		CommonUtils.markPushEnd(topic);
	}

	private class Producer implements Runnable {

		final String producerId;
		final QueueService queue;
		final int messageCount;

		Producer(String producerId, String topic, int messageCount) throws IOException {
			this.producerId = producerId;
			this.queue = new FileBasedQueueService(topic, queueSize);
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

		private void produce(String producerId, QueueService queue, int messageCount)
				throws IOException, InterruptedException {

			int currentMessageCount = 0;
			while (currentMessageCount < messageCount) {
				String msg = String.format("%s-%s", producerId, currentMessageCount);

				try {

					boolean isSuccessful = queue.push(msg) != FileBasedQueueService.INVALID_POSITON;
					if (isSuccessful) {
						currentMessageCount++;
					}

				} catch (BufferOverflowException exception) {
					System.out.println("Memory limit exceeded. Total messages pushed - " + currentMessageCount);
					break;
				}

			}

			System.out.println(String.format("Total %s messages pushed by %s to %s", currentMessageCount, producerId,
					queue.getQueueName()));
		}
	}

	public void setDatasourceSize(int dataSourceSize) {
		this.queueSize = dataSourceSize;
	}
}

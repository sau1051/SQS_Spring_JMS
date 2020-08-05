package com.saurabh.learning.sqs.config;

import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class QueueConfig {

	private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/569889921972/abcd";
	private static final String QUEUE_NAME = "abcd";

	public static void main(String[] args) {
		BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("AKIAYJMAWDO2LH2ATAE6",
				"CqSEJCoaBGx3OEQjMDpBeETE2AQQIaQfEkFgSQLE");
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials)).build();

		//ListQueuesResult queues = sqs.listQueues();
		
		

		// Creating a Queue
/*
		CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME).addAttributesEntry("DelaySeconds", "60")
				.addAttributesEntry("MessageRetentionPeriod", "86400");

		try {
			sqs.createQueue(create_request);
		} catch (AmazonSQSException e) {
			if (!e.getErrorCode().equals("QueueAlreadyExists")) {
				throw e;
			}
		}

		// Get the URL for a queue
		String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

		// Delete the Queue
		sqs.deleteQueue(queue_url);

		sqs.createQueue("Queue1" + new Date().getTime());
		sqs.createQueue("Queue2" + new Date().getTime());
		sqs.createQueue("MyQueue" + new Date().getTime());

		// List your queues
		ListQueuesResult lq_result = sqs.listQueues();

		System.out.println("Your SQS Queue URLs:");
		for (String url : lq_result.getQueueUrls()) {
			System.out.println(url);
		}

		// List queues with filters
		String name_prefix = "Queue";
		lq_result = sqs.listQueues(new ListQueuesRequest(name_prefix));
		System.out.println("Queue URLs with prefix: " + name_prefix);
		for (String url : lq_result.getQueueUrls()) {
			System.out.println(url);
		}*/
		
		
		
		String uri = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
		
/*		// Send single message
		SendMessageRequest send_msg_request = new SendMessageRequest()
		        .withQueueUrl(uri)
		        .withMessageBody("hello world")
		        .withDelaySeconds(5);
		sqs.sendMessage(send_msg_request);
		
		// Send messages in batch
		SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
		        .withQueueUrl(uri)
		        .withEntries(
		                new SendMessageBatchRequestEntry(
		                        "msg_1", "Hello from message 1"),
		                new SendMessageBatchRequestEntry(
		                        "msg_2", "Hello from message 2")
		                        .withDelaySeconds(10));
		sqs.sendMessageBatch(send_batch_request);*/
		
		
		
		// Receive messages from queue
		ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
		        .withQueueUrl(uri)
		        .withWaitTimeSeconds(20)
		        .withMaxNumberOfMessages(10);
		ReceiveMessageResult messageResult = sqs.receiveMessage(receive_request);
		
		List<Message> messageList = messageResult.getMessages();
		
		for(Message m : messageList) {
			System.out.println(m.getMessageId() + "::" + m.getMD5OfBody() + "::" + m.getBody());
		}

	}

}

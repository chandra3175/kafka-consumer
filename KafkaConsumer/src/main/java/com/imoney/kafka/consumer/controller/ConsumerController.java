package com.imoney.kafka.consumer.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.imoney.kafkaclient.dto.User;

@RestController
@RequestMapping("test/kafka")
public class ConsumerController {

	@Value("${kafka.user.details.topic}")
	public String topicName;

	@Autowired
	private ConcurrentKafkaListenerContainerFactory<?, ?> factory;

	@GetMapping("/consumer/message")
	public List<User> receiveMessage() {
		List<User> students = new ArrayList<>();
		ConsumerFactory<?, ?> consumerFactory = factory.getConsumerFactory();
		@SuppressWarnings("unchecked")
		Consumer<String, User> consumer = (Consumer<String, User>) consumerFactory.createConsumer();
		try {
			consumer.subscribe(Arrays.asList(topicName));
			ConsumerRecords<String, User> consumerRecords = consumer.poll(10000);
			Iterable<ConsumerRecord<String, User>> records = consumerRecords.records(topicName);
			Iterator<ConsumerRecord<String, User>> iterator = records.iterator();

			while (iterator.hasNext()) {
				students.add(iterator.next().value());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return students;
	}
}

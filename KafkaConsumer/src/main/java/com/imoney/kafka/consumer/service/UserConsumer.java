package com.imoney.kafka.consumer.service;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.imoney.kafkaclient.consumer.impl.BaseConsumer;
import com.imoney.kafkaclient.dto.User;
import com.imoney.kafkaclient.utill.KafkaUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@Lazy(false)
public class UserConsumer extends BaseConsumer<String, String> {

	@Value("${user.update.consumer.poll.interval:500}")
	public int pollInterval;

	@Autowired
	private Gson gson;

	@Autowired
	private Environment environment;

	@Override
	public List<String> getTopics() {
		return Arrays.asList(environment.getProperty("kafka.user.details.topic"));
	}

	@PostConstruct
	public void startConsumer() {
		this.configure(KafkaUtil.getConsumerProperties(environment));
		new Thread(this).start();
	}

	@Override
	public void subscriber(ConsumerRecords<String, String> records) {
		try {
			if (records != null && !records.isEmpty()) {
				log.info("UserUpdateConsumer:: Received " + records.count() + " record(s) to process");

				for (ConsumerRecord<String, String> record : records) {

					User userDto = gson.fromJson(record.value(), User.class);
					log.info("UserUpdateConsumer:: Request : [" + userDto + "]");

					if (StringUtils.isNotBlank(userDto.getRollNumber())) {
						log.info(userDto.toString());
					} else {
						log.info("Update Request is not valid. Not processing data!");
					}
				}
			}
		} catch (Exception e) {
			log.error("UserUpdateConsumer:: Error while Processing User Update Dto : ", e);
		}

	}

	@Override
	public int getPollDuration() {
		return pollInterval;
	}

}

package com.sck.gcp.messagereciever;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.activemq.Message;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.sck.gcp.processor.FileProcessor;
import com.sck.gcp.service.BigQueryService;
import com.sck.gcp.service.CloudStorageService;

@Component
public class ActiveMQMessageReceiver {
	private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQMessageReceiver.class);

	@Autowired
	private FileProcessor fileProcessor;
	
	@Autowired
	private BigQueryService bigQueryService;
	
	@Value("${com.sck.upload.dir:upload}")
	private String uploadFolder;
	
	@Value("${com.sck.upload.backup.dir:backup}")
	private String backupFolder;
	
	@Value("${sck.gcp.bigquery.table}")
	private String bqTable;
	
	


	@Autowired
	private CloudStorageService cloudStorageService;

	@JmsListener(destination = "${inbound.endpoint}", containerFactory = "jmsListenerContainerFactory")
	public void receiveMessage(Message msg) throws JMSException {
		try {
			String xml = ((TextMessage) msg).getText();
			//fileProcessor.convertToJSONL(xml);
			LOGGER.info("readFile() completed");
			
			JSONArray jsonProducts = fileProcessor.convertToJSONs(xml);
			String jsonl = fileProcessor.convertToJSONL(jsonProducts);
			LOGGER.info("convertToJSONL() completed");
			
			byte[] arr = jsonl.getBytes();
			String uploadFile = cloudStorageService.getFilePath(backupFolder, "product");
			cloudStorageService.uploadToCloudStorage(uploadFile, arr);
			LOGGER.info("File uploaded to bucket with name " + uploadFile);
			
			InputStream dataStream = new ByteArrayInputStream(jsonl.getBytes());
			ListenableFuture<Job> payloadJob = bigQueryService.writeFileToTable(bqTable, dataStream,
					FormatOptions.json());
			LOGGER.info("Upload completed to Table:" + bqTable);
			
		} catch (JMSException e) {
			LOGGER.error("JMSException occured", e);
		}

	}

}

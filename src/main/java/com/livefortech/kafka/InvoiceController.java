package com.livefortech.kafka;

import java.net.URI;
import java.net.URISyntaxException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("invoice")
public class InvoiceController {
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate ;
	
	@Autowired
	KafkaTemplate<String, Invoice> kafkaInvoiceTemplate ;
	
	@Autowired
	RestTemplate restTemplate;
	
	private final String TOPIC_NAME = "sample";
	
	private final String INVOICE_TOPIC_NAME = "travel-invoice";
	
	@PostMapping("/publish/message")
	public String postInvoice(@RequestBody(required = true) String message) {
		kafkaTemplate.send(TOPIC_NAME, message);		
		return "Published Message Successfully";
		
	}
	
	@PostMapping("/publish")
	public String postInvoice(@RequestBody(required = true) Invoice invoice) {
		kafkaInvoiceTemplate.send(INVOICE_TOPIC_NAME, invoice);		
		return "Published invoice Successfully for invoice : "+invoice.getId();
		
	}
	
	@PostMapping("/order")
	public String callingServiceB(@RequestBody(required = true) Invoice invoice) throws URISyntaxException {

		String url = "http://localhost:8081/ServiceB/receive";
		URI uri = new URI(url);
		HttpEntity<Invoice> invoiceRequestbody = new HttpEntity<>(invoice,null);
		restTemplate.exchange(uri, HttpMethod.POST, invoiceRequestbody, Void.class);
		return "Successfully called Service B";
		
	}


}
 
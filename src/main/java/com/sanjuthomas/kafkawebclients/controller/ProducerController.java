package com.sanjuthomas.kafkawebclients.controller;

import com.sanjuthomas.kafkawebclients.model.ProduceRequest;
import com.sanjuthomas.kafkawebclients.model.ProduceResult;
import com.sanjuthomas.kafkawebclients.service.KafkaProducerOperations;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/produce")
public class ProducerController {

    private final KafkaProducerOperations kafkaProducerService;

    public ProducerController(KafkaProducerOperations kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public Mono<ProduceResult> produce(@RequestBody ProduceRequest request) {
        return kafkaProducerService.produce(request);
    }
}

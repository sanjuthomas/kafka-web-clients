package com.example.kafkawebclients.controller;

import com.example.kafkawebclients.model.ProduceRequest;
import com.example.kafkawebclients.model.ProduceResult;
import com.example.kafkawebclients.service.KafkaProducerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/produce")
public class ProducerController {

    private final KafkaProducerService kafkaProducerService;

    public ProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping
    public Mono<ProduceResult> produce(@RequestBody ProduceRequest request) {
        return kafkaProducerService.produce(request);
    }
}

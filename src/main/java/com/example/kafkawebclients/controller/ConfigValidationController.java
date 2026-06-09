package com.example.kafkawebclients.controller;

import com.example.kafkawebclients.model.ConfigValidationResult;
import com.example.kafkawebclients.model.StreamConfig;
import com.example.kafkawebclients.service.KafkaConnectivityOperations;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/config")
public class ConfigValidationController {

    private final KafkaConnectivityOperations kafkaConnectivityService;

    public ConfigValidationController(KafkaConnectivityOperations kafkaConnectivityService) {
        this.kafkaConnectivityService = kafkaConnectivityService;
    }

    @PostMapping("/validate")
    public Mono<ConfigValidationResult> validate(@RequestBody StreamConfig config) {
        return kafkaConnectivityService.validate(config);
    }
}

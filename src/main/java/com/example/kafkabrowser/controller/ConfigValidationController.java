package com.example.kafkabrowser.controller;

import com.example.kafkabrowser.model.ConfigValidationResult;
import com.example.kafkabrowser.model.StreamConfig;
import com.example.kafkabrowser.service.KafkaConnectivityService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/config")
public class ConfigValidationController {

    private final KafkaConnectivityService kafkaConnectivityService;

    public ConfigValidationController(KafkaConnectivityService kafkaConnectivityService) {
        this.kafkaConnectivityService = kafkaConnectivityService;
    }

    @PostMapping("/validate")
    public Mono<ConfigValidationResult> validate(@RequestBody StreamConfig config) {
        return kafkaConnectivityService.validate(config);
    }
}

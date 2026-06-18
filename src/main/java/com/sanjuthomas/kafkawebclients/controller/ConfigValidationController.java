package com.sanjuthomas.kafkawebclients.controller;

import com.sanjuthomas.kafkawebclients.model.ClusterConnectionRequest;
import com.sanjuthomas.kafkawebclients.model.ConfigValidationResult;
import com.sanjuthomas.kafkawebclients.model.StreamConfig;
import com.sanjuthomas.kafkawebclients.service.KafkaConnectivityOperations;
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

    @PostMapping("/validate-cluster")
    public Mono<ConfigValidationResult> validateCluster(@RequestBody ClusterConnectionRequest request) {
        return kafkaConnectivityService.validateCluster(request);
    }
}

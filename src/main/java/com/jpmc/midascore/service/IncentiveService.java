package com.jpmc.midascore.service;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class IncentiveService {
    private final RestTemplate restTemplate;
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    public IncentiveService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public float fetchIncentive(Transaction transaction) {
        ResponseEntity<Map> response = restTemplate.postForEntity(INCENTIVE_API_URL, transaction, Map.class);

        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            return ((Number) response.getBody().get("amount")).floatValue();
        }
        return 0.0f; // Default incentive if API call fails
    }
}

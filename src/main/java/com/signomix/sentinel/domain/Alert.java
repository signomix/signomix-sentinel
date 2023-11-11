package com.signomix.sentinel.domain;

public record Alert(
        Long id,
        int level,
        String eui,
        String message) {
}

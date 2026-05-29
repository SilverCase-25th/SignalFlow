package dev.signalflow.ingest.service;

import dev.signalflow.ingest.dto.IngestResponse;
import dev.signalflow.ingest.dto.LogRequest;
import dev.signalflow.ingest.dto.TopGroupsResponse;
import dev.signalflow.ingest.repository.LogEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Orchestrates the ingestion pipeline:
 * validate (done by Spring) → normalize → fingerprint → persist → aggregate.
 */
@Service
public class LogIngestionService {

    private final NormalizationService normalization;
    private final FingerprintService fingerprinting;
    private final LogEventRepository repository;

    public LogIngestionService(
            NormalizationService normalization,
            FingerprintService fingerprinting,
            LogEventRepository repository) {
        this.normalization = normalization;
        this.fingerprinting = fingerprinting;
        this.repository = repository;
    }

    /**
     * Ingests a single validated log entry.
     * The raw event and the minute-bucket aggregate are written in one transaction.
     */
    @Transactional
    public IngestResponse ingest(LogRequest request) {
        String normalized = normalization.normalize(request.message());
        String fp = fingerprinting.fingerprint(request.service(), normalized, request.severity());
        repository.saveLogEvent(request, normalized, fp);
        repository.upsertMinuteAggregate(request, normalized, fp);
        return new IngestResponse(fp, normalized);
    }

    /**
     * Returns the top fingerprint groups observed in the given time window.
     *
     * @param window one of: {@code 15m}, {@code 1h}, {@code 24h}
     */
    public TopGroupsResponse topGroups(String window) {
        Duration duration = parseWindow(window);
        Instant since = Instant.now().minus(duration);
        List<TopGroupsResponse.GroupEntry> groups = repository.topGroups(since);
        return new TopGroupsResponse(window, groups);
    }

    private Duration parseWindow(String window) {
        return switch (window) {
            case "15m" -> Duration.ofMinutes(15);
            case "1h"  -> Duration.ofHours(1);
            case "24h" -> Duration.ofHours(24);
            default -> throw new IllegalArgumentException(
                    "Unsupported window '" + window + "'. Valid values: 15m, 1h, 24h.");
        };
    }
}

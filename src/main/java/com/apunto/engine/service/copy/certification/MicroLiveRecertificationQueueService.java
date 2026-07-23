package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.MicroLiveRecertificationRequestEntity;
import com.apunto.engine.repository.MicroLiveRecertificationRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.HexFormat;

@Service
@RequiredArgsConstructor
public class MicroLiveRecertificationQueueService implements MicroLiveRecertificationQueue {

    private final MicroLiveRecertificationRequestRepository repository;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public MicroLiveRecertificationRequestEntity enqueue(MicroLiveRecertificationRequest request) {
        String key = idempotencyKey(request);
        MicroLiveRecertificationRequestEntity existing = repository.findByIdempotencyKey(key).orElse(null);
        if (existing != null) return existing;
        OffsetDateTime now = OffsetDateTime.now();
        repository.insertIfAbsent(UUID.randomUUID(), request.certificationId(), request.walletId(),
                request.strategyCode(), value(request.strategyVersion(), "UNVERSIONED"),
                request.userId(), request.executionAccountId(), Math.max(1, request.priority()),
                value(request.reasonCode(), "MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY"), key, now);
        return repository.findByIdempotencyKey(key).orElseThrow();
    }

    @Transactional
    public Optional<MicroLiveRecertificationRequestEntity> claimNext() {
        Optional<MicroLiveRecertificationRequestEntity> candidate = repository.findNextPendingForUpdate();
        candidate.ifPresent(request -> {
            OffsetDateTime now = OffsetDateTime.now();
            request.setStatus(MicroLiveRecertificationRequestEntity.Status.CLAIMED);
            request.setAttempts(request.getAttempts() + 1);
            request.setClaimedAt(now);
            request.setReasonCode("MICRO_LIVE_RECERTIFICATION_ADMISSION_IN_PROGRESS");
            request.setUpdatedAt(now);
            repository.saveAndFlush(request);
        });
        return candidate;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void defer(UUID requestId, String reasonCode) {
        repository.findById(requestId).ifPresent(request -> {
            request.setStatus(MicroLiveRecertificationRequestEntity.Status.PENDING_CAPACITY);
            request.setReasonCode(value(reasonCode, "MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY"));
            request.setNextAttemptAt(OffsetDateTime.now().plusSeconds(15));
            request.setClaimedAt(null);
            repository.saveAndFlush(request);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finish(UUID requestId, MicroLiveRecertificationRequestEntity.Status status,
                       String reasonCode, Long allocationId) {
        repository.findById(requestId).ifPresent(request -> {
            request.setStatus(status);
            request.setReasonCode(reasonCode);
            request.setUserCopyAllocationId(allocationId);
            request.setCompletedAt(OffsetDateTime.now());
            repository.saveAndFlush(request);
        });
    }

    static String idempotencyKey(MicroLiveRecertificationRequest request) {
        String source = String.join("|",
                value(request.certificationId(), ""), value(request.userId(), ""),
                value(request.executionAccountId(), ""), value(request.walletId(), ""),
                value(request.strategyCode(), ""), value(request.strategyVersion(), ""));
        try {
            return "RECERT:" + HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String value(Object value, String fallback) {
        String text = value == null ? "" : value.toString().trim();
        return text.isEmpty() ? fallback : text;
    }
}

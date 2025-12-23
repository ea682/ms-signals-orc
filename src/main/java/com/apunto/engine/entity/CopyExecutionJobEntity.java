package com.apunto.engine.entity;

import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobErrorCategory;
import com.apunto.engine.jobs.model.CopyJobStatus;
import jakarta.persistence.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "copy_execution_job",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_copy_execution_job_origin_user_action",
                columnNames = {"origin_id", "user_id", "action"}
        )
)
public class CopyExecutionJobEntity {

    @Id
    @GeneratedValue
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Column(name = "origin_id", nullable = false, length = 80)
    private String originId;

    @Column(name = "user_id", nullable = false, length = 80)
    private String userId;

    @Enumerated(EnumType.STRING)
    @Column(name = "action", nullable = false, length = 16)
    private CopyJobAction action;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private CopyJobStatus status;

    @Column(name = "attempt", nullable = false)
    private int attempt;

    @Column(name = "next_run_at", nullable = false)
    private OffsetDateTime nextRunAt;

    @Column(name = "locked_at")
    private OffsetDateTime lockedAt;

    @Column(name = "locked_by", length = 128)
    private String lockedBy;

    @Column(name = "payload", columnDefinition = "text", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "last_error_category", nullable = false, length = 32)
    private CopyJobErrorCategory lastErrorCategory;

    @Column(name = "last_error_message", columnDefinition = "text")
    private String lastErrorMessage;

    @Column(name = "last_error_at")
    private OffsetDateTime lastErrorAt;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (this.id == null) this.id = UUID.randomUUID();
        if (this.createdAt == null) this.createdAt = now;
        this.updatedAt = now;
        if (this.status == null) this.status = CopyJobStatus.PENDING;
        if (this.lastErrorCategory == null) this.lastErrorCategory = CopyJobErrorCategory.NONE;
        if (this.nextRunAt == null) this.nextRunAt = now;
    }

    @PreUpdate
    void preUpdate() {
        this.updatedAt = OffsetDateTime.now();
    }

    // getters/setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public String getOriginId() { return originId; }
    public void setOriginId(String originId) { this.originId = originId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public CopyJobAction getAction() { return action; }
    public void setAction(CopyJobAction action) { this.action = action; }
    public CopyJobStatus getStatus() { return status; }
    public void setStatus(CopyJobStatus status) { this.status = status; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public OffsetDateTime getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(OffsetDateTime nextRunAt) { this.nextRunAt = nextRunAt; }
    public OffsetDateTime getLockedAt() { return lockedAt; }
    public void setLockedAt(OffsetDateTime lockedAt) { this.lockedAt = lockedAt; }
    public String getLockedBy() { return lockedBy; }
    public void setLockedBy(String lockedBy) { this.lockedBy = lockedBy; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public CopyJobErrorCategory getLastErrorCategory() { return lastErrorCategory; }
    public void setLastErrorCategory(CopyJobErrorCategory lastErrorCategory) { this.lastErrorCategory = lastErrorCategory; }
    public String getLastErrorMessage() { return lastErrorMessage; }
    public void setLastErrorMessage(String lastErrorMessage) { this.lastErrorMessage = lastErrorMessage; }
    public OffsetDateTime getLastErrorAt() { return lastErrorAt; }
    public void setLastErrorAt(OffsetDateTime lastErrorAt) { this.lastErrorAt = lastErrorAt; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(OffsetDateTime createdAt) { this.createdAt = createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime updatedAt) { this.updatedAt = updatedAt; }
}

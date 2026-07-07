package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CopyPromotionAuditRepository extends JpaRepository<CopyPromotionAuditEntity, Long> {
}

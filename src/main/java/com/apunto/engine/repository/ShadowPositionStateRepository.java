package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowPositionStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.List;
import java.util.UUID;

@Repository
public interface ShadowPositionStateRepository extends JpaRepository<ShadowPositionStateEntity, UUID> {

    Optional<ShadowPositionStateEntity> findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus(
            Long shadowAllocationId,
            String parsymbol,
            String positionSide,
            String status
    );

    List<ShadowPositionStateEntity> findAllByShadowAllocationIdAndParsymbolAndStatus(
            Long shadowAllocationId,
            String parsymbol,
            String status
    );

    Optional<ShadowPositionStateEntity> findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatus(
            Long walletProfileId,
            String parsymbol,
            String positionSide,
            String status
    );

    Optional<ShadowPositionStateEntity> findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc(
            Long shadowAllocationId,
            String parsymbol,
            String positionSide,
            String status
    );

    Optional<ShadowPositionStateEntity> findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc(
            Long walletProfileId,
            String parsymbol,
            String positionSide,
            String status
    );

    List<ShadowPositionStateEntity> findAllByWalletProfileIdAndParsymbolAndStatus(
            Long walletProfileId,
            String parsymbol,
            String status
    );

    @Query("""
            select count(s)
            from ShadowPositionStateEntity s
            where s.shadowAllocationId = :shadowAllocationId
              and s.status = 'CLOSED'
              and s.closedAt is not null
            """)
    long countClosedPositions(@Param("shadowAllocationId") Long shadowAllocationId);

    @Query("""
            select sum(s.realizedPnlUsd)
            from ShadowPositionStateEntity s
            where s.shadowAllocationId = :shadowAllocationId
              and s.status = 'CLOSED'
              and s.closedAt is not null
            """)
    BigDecimal sumClosedRealizedPnlUsd(@Param("shadowAllocationId") Long shadowAllocationId);

    @Query("""
            select count(s)
            from ShadowPositionStateEntity s
            where s.shadowAllocationId = :shadowAllocationId
              and s.status = 'OPEN'
            """)
    long countOpenPositions(@Param("shadowAllocationId") Long shadowAllocationId);

    @Query("""
            select count(s)
            from ShadowPositionStateEntity s
            where s.walletProfileId = :walletProfileId
              and s.status = 'OPEN'
            """)
    long countOpenPositionsByWalletProfileId(@Param("walletProfileId") Long walletProfileId);

    @Query("""
            select count(s)
            from ShadowPositionStateEntity s
            where s.walletProfileId = :walletProfileId
              and s.status = 'CLOSED'
              and s.closedAt is not null
            """)
    long countClosedPositionsByWalletProfileId(@Param("walletProfileId") Long walletProfileId);

    @Query("""
            select sum(s.realizedPnlUsd)
            from ShadowPositionStateEntity s
            where s.walletProfileId = :walletProfileId
              and s.status = 'CLOSED'
              and s.closedAt is not null
            """)
    BigDecimal sumClosedRealizedPnlUsdByWalletProfileId(@Param("walletProfileId") Long walletProfileId);

    @Query("""
            select sum(s.slippageUsd)
            from ShadowPositionStateEntity s
            where s.walletProfileId = :walletProfileId
            """)
    BigDecimal sumSlippageUsdByWalletProfileId(@Param("walletProfileId") Long walletProfileId);
}

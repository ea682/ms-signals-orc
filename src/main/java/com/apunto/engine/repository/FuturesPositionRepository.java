package com.apunto.engine.repository;

import com.apunto.engine.entity.FuturesPositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface FuturesPositionRepository extends JpaRepository<FuturesPositionEntity, UUID> {

    Optional<FuturesPositionEntity> findByIdFuturesPosition(UUID idFuturesPosition);

    Optional<FuturesPositionEntity> findByPlatformAndExternalId(String platform, String externalId);

    @Query(value = """
            select *
            from futuros_operaciones.futures_position fp
            where fp.account_id = :accountId
              and fp.status = cast(:status as futuros_operaciones.position_status)
              and fp.is_active = true
            order by fp.created_at asc
            """, nativeQuery = true)
    List<FuturesPositionEntity> findAllOpenByAccountIdOrderByCreatedAtAsc(
            @Param("accountId") String accountId,
            @Param("status") String status
    );
    @Query(value = """
            select *
            from futuros_operaciones.futures_position fp
            where fp.platform = :platform
              and fp.status = cast(:status as futuros_operaciones.position_status)
              and fp.is_active = true
            order by fp.created_at asc
            """, nativeQuery = true)
    List<FuturesPositionEntity> findAllActiveByPlatformAndStatus(
            @Param("platform") String platform,
            @Param("status") String status
    );


    @Query(value = """
            select *
            from futuros_operaciones.futures_position fp
            where fp.platform = :platform
              and fp.account_id = :accountId
              and fp.symbol = :symbol
              and fp.side = cast(:side as futuros_operaciones.position_side)
              and fp.status = cast(:status as futuros_operaciones.position_status)
              and fp.is_active = true
            order by fp.created_at desc
            limit 1
            """, nativeQuery = true)
    Optional<FuturesPositionEntity> findLatestActiveByPlatformAccountSymbolSide(
            @Param("platform") String platform,
            @Param("accountId") String accountId,
            @Param("symbol") String symbol,
            @Param("side") String side,
            @Param("status") String status
    );

}

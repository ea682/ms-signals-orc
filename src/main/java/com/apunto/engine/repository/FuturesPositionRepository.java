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
}

package com.apunto.engine.service.copy.account;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class PostgresMicroLiveCapacitySnapshotStore implements MicroLiveCapacitySnapshotStore {

    private final JdbcTemplate jdbc;

    @Override
    public void save(MicroLiveCapacitySnapshot value) {
        jdbc.update("""
                insert into futuros_operaciones.micro_live_account_capacity (
                    execution_account_id, asset, authoritative_equity_usd, available_balance_usd,
                    safety_buffer_usd, eligible_capital_usd, budget_per_allocation_usd,
                    theoretical_capacity, effective_capacity, configured_max,
                    reserved_recertification_slots, observed_at, valid_until, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                on conflict (execution_account_id) do update set
                    asset = excluded.asset,
                    authoritative_equity_usd = excluded.authoritative_equity_usd,
                    available_balance_usd = excluded.available_balance_usd,
                    safety_buffer_usd = excluded.safety_buffer_usd,
                    eligible_capital_usd = excluded.eligible_capital_usd,
                    budget_per_allocation_usd = excluded.budget_per_allocation_usd,
                    theoretical_capacity = excluded.theoretical_capacity,
                    effective_capacity = excluded.effective_capacity,
                    configured_max = excluded.configured_max,
                    reserved_recertification_slots = excluded.reserved_recertification_slots,
                    observed_at = excluded.observed_at,
                    valid_until = excluded.valid_until,
                    updated_at = now()
                """,
                value.executionAccountId(), value.asset(), value.authoritativeEquityUsd(),
                value.availableBalanceUsd(), value.safetyBufferUsd(), value.eligibleCapitalUsd(),
                value.budgetPerAllocationUsd(), value.theoreticalCapacity(), value.effectiveCapacity(),
                value.configuredMax(), value.reservedRecertificationSlots(), value.observedAt(),
                value.validUntil());
    }
}

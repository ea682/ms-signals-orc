package com.apunto.engine.service.copy.account;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.shared.dto.ApiResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.UUID;

@Service
public class ExecutionAccountIsolationVerifier {

    private static final String EXCHANGE = "BINANCE";
    private static final long VERIFICATION_TTL_HOURS = 24;

    private final UserApiKeyRepository repository;
    private final BinanceClient binanceClient;

    public ExecutionAccountIsolationVerifier(
            UserApiKeyRepository repository,
            @Qualifier("binanceInfoClient") BinanceClient binanceClient) {
        this.repository = repository;
        this.binanceClient = binanceClient;
    }

    @Transactional
    public ExecutionAccountIsolationDecision verify(UUID userId, UserApiKeyEntity selected) {
        if (selected == null || selected.getAccountPurpose() == null) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNT_IDENTITY_UNAVAILABLE");
        }
        ExecutionAccountPurpose counterpartPurpose = selected.getAccountPurpose() == ExecutionAccountPurpose.LIVE
                ? ExecutionAccountPurpose.MICRO_LIVE : ExecutionAccountPurpose.LIVE;
        UserApiKeyEntity counterpart = repository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                        userId, EXCHANGE, counterpartPurpose).orElse(null);
        if (counterpart == null) return ExecutionAccountIsolationDecision.allowed();
        if (same(selected.getApiKey(), counterpart.getApiKey())) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNTS_NOT_ISOLATED");
        }
        if (sameRef(selected, counterpart)) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNTS_NOT_ISOLATED");
        }
        if (fresh(selected) && fresh(counterpart)) return ExecutionAccountIsolationDecision.allowed();

        String selectedRef = probe(userId, selected);
        String counterpartRef = probe(userId, counterpart);
        if (selectedRef == null || counterpartRef == null) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNT_IDENTITY_UNAVAILABLE");
        }
        if (selectedRef.equals(counterpartRef)) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNTS_NOT_ISOLATED");
        }
        if (changed(selected, selectedRef) || changed(counterpart, counterpartRef)) {
            return ExecutionAccountIsolationDecision.blocked("EXECUTION_ACCOUNT_REBIND_FORBIDDEN");
        }
        OffsetDateTime now = OffsetDateTime.now();
        selected.setExchangeAccountRef(selectedRef);
        selected.setIdentityVerifiedAt(now);
        counterpart.setExchangeAccountRef(counterpartRef);
        counterpart.setIdentityVerifiedAt(now);
        repository.saveAndFlush(selected);
        repository.saveAndFlush(counterpart);
        return ExecutionAccountIsolationDecision.allowed();
    }

    private String probe(UUID userId, UserApiKeyEntity account) {
        try {
            ApiResponse<FuturesAssetBalanceClientResponse> response = binanceClient.assetBalance(
                    account.getApiKey(), account.getApiSecret(), null, userId.toString(), null,
                    "execution-account-identity-" + UUID.randomUUID(), "USDC");
            if (response == null || response.getData() == null || !successful(response)) return null;
            return normalize(response.getData().getAccountAlias());
        } catch (RuntimeException unavailable) {
            return null;
        }
    }

    private static boolean successful(ApiResponse<?> response) {
        int code = response.getStatusCode();
        return code == 0 || (code >= 200 && code < 300);
    }

    private static boolean fresh(UserApiKeyEntity account) {
        OffsetDateTime verified = account.getIdentityVerifiedAt();
        if (account.getExchangeAccountRef() == null || verified == null
                || verified.isBefore(OffsetDateTime.now().minusHours(VERIFICATION_TTL_HOURS))) return false;
        return account.getUpdatedAt() == null || !account.getUpdatedAt().isAfter(verified);
    }

    private static boolean changed(UserApiKeyEntity account, String probed) {
        String current = normalize(account.getExchangeAccountRef());
        return current != null && !current.equals(probed);
    }

    private static boolean sameRef(UserApiKeyEntity left, UserApiKeyEntity right) {
        String first = normalize(left.getExchangeAccountRef());
        return first != null && first.equals(normalize(right.getExchangeAccountRef()));
    }

    private static boolean same(String left, String right) {
        return left != null && right != null && left.trim().equals(right.trim());
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) return null;
        return value.trim().toUpperCase(Locale.ROOT);
    }
}

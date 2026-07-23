package com.apunto.engine.service.copy.account;

import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserApiKeyRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.UUID;

@Service
public class ExecutionAccountResolver {

    private static final String EXCHANGE = "BINANCE";
    private final UserApiKeyRepository repository;
    private final ExecutionAccountIsolationVerifier isolationVerifier;

    @Autowired
    public ExecutionAccountResolver(UserApiKeyRepository repository,
                                    ExecutionAccountIsolationVerifier isolationVerifier) {
        this.repository = repository;
        this.isolationVerifier = isolationVerifier;
    }

    ExecutionAccountResolver(UserApiKeyRepository repository) {
        this(repository, null);
    }

    public ExecutionAccountResolution resolve(UserCopyAllocationEntity allocation) {
        if (allocation == null || allocation.getIdUser() == null) {
            return ExecutionAccountResolution.blocked(null, "EXECUTION_ACCOUNT_ALLOCATION_INVALID");
        }
        return resolve(allocation.getIdUser(), allocation.getExecutionMode(), allocation.getExchangeAccountId());
    }

    public ExecutionAccountResolution resolve(UUID userId, String executionMode, UUID boundAccountId) {
        ExecutionAccountPurpose purpose = purpose(executionMode);
        if (userId == null || purpose == null) {
            return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_ALLOCATION_INVALID");
        }
        if (boundAccountId != null) {
            UserApiKeyEntity bound = repository.findById(boundAccountId).orElse(null);
            if (bound == null) return missing(purpose);
            if (bound.getUser() == null || !userId.equals(bound.getUser().getId())
                    || !EXCHANGE.equalsIgnoreCase(text(bound.getExchange()))) {
                return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_PURPOSE_MISMATCH");
            }
            if (bound.getAccountPurpose() != purpose) {
                return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_PURPOSE_MISMATCH");
            }
            return validate(userId, purpose, bound);
        }

        UserApiKeyEntity selected = repository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(userId, EXCHANGE, purpose)
                .orElse(null);
        if (selected != null) return validate(userId, purpose, selected);
        UserApiKeyEntity inactive = repository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurpose(userId, EXCHANGE, purpose)
                .orElse(null);
        return inactive == null
                ? missing(purpose)
                : ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_INACTIVE");
    }

    private ExecutionAccountResolution validate(UUID userId, ExecutionAccountPurpose purpose,
                                                UserApiKeyEntity account) {
        if (!account.isActive()) {
            return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_INACTIVE");
        }
        if (blank(account.getApiKey()) || blank(account.getApiSecret())) {
            return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNT_CREDENTIALS_INVALID");
        }
        if (isolationVerifier != null) {
            ExecutionAccountIsolationDecision isolation = isolationVerifier.verify(userId, account);
            if (!isolation.isolated()) {
                return ExecutionAccountResolution.blocked(purpose, isolation.reasonCode());
            }
        } else {
            ExecutionAccountPurpose otherPurpose = purpose == ExecutionAccountPurpose.LIVE
                    ? ExecutionAccountPurpose.MICRO_LIVE : ExecutionAccountPurpose.LIVE;
            UserApiKeyEntity other = repository
                    .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                            userId, EXCHANGE, otherPurpose).orElse(null);
            if (other != null && (same(account.getApiKey(), other.getApiKey())
                    || sameNonBlank(account.getExchangeAccountRef(), other.getExchangeAccountRef()))) {
                return ExecutionAccountResolution.blocked(purpose, "EXECUTION_ACCOUNTS_NOT_ISOLATED");
            }
        }
        return ExecutionAccountResolution.allowed(purpose, account);
    }

    private static ExecutionAccountResolution missing(ExecutionAccountPurpose purpose) {
        return ExecutionAccountResolution.blocked(purpose,
                purpose == ExecutionAccountPurpose.MICRO_LIVE
                        ? "MICRO_LIVE_EXECUTION_ACCOUNT_MISSING"
                        : "LIVE_EXECUTION_ACCOUNT_MISSING");
    }

    private static ExecutionAccountPurpose purpose(String executionMode) {
        String normalized = text(executionMode).toUpperCase(Locale.ROOT).replace('-', '_');
        if ("MICRO_LIVE".equals(normalized)) return ExecutionAccountPurpose.MICRO_LIVE;
        if ("LIVE".equals(normalized)) return ExecutionAccountPurpose.LIVE;
        return null;
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static boolean same(String left, String right) {
        return left != null && right != null && left.trim().equals(right.trim());
    }

    private static boolean sameNonBlank(String left, String right) {
        return !blank(left) && !blank(right) && left.trim().equalsIgnoreCase(right.trim());
    }
}

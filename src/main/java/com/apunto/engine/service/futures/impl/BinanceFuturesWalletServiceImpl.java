package com.apunto.engine.service.futures.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientRequest;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.service.futures.BinanceFuturesWalletService;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.FuturesWalletClientException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.LogFmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class BinanceFuturesWalletServiceImpl implements BinanceFuturesWalletService {

    private static final String ASSET_BNB = "BNB";

    private final BinanceClient binanceClient;
    private final ObjectMapper objectMapper;

    @Override
    public FuturesAssetBalanceClientResponse getAssetBalance(UserDetailDto userDetail, String asset) {
        validateUserCredentials(userDetail);
        String normalizedAsset = normalizeWalletAsset(asset);
        String userId = userId(userDetail);

        try {
            ApiResponse<FuturesAssetBalanceClientResponse> response = binanceClient.assetBalance(
                    userDetail.getUserApiKey().getApiKey(),
                    userDetail.getUserApiKey().getApiSecret(),
                    null,
                    userId,
                    null,
                    traceId(),
                    normalizedAsset
            );
            FuturesAssetBalanceClientResponse balance = unwrap(response, "futures.wallet.balance");
            log.info("event=futures.wallet.balance.ok userId={} asset={} availableBalance={} walletBalance={} friendlyStep=ya_se_cuanto_saldo_tiene_esta_moneda",
                    userId, normalizedAsset, balance.getAvailableBalance(), balance.getWalletBalance());
            return balance;
        } catch (RestClientResponseException ex) {
            BinanceHttpError err = parseBinanceHttpError(ex);
            Map<String, Object> details = walletDetails(userId, normalizedAsset, err);
            log.warn("event=futures.wallet.balance.fail userId={} asset={} httpStatus={} errorCode={} binanceCode={} binanceMsg=\"{}\" friendlyStep=no_pude_leer_el_saldo_en_binance",
                    userId, normalizedAsset, err.httpStatus(), safeNull(err.errorCode()), safeNull(err.binanceCode()), safeLog(err.binanceMsg()));
            throw new FuturesWalletClientException("No se pudo consultar saldo Futures de " + normalizedAsset, ex, details);
        } catch (ResourceAccessException ex) {
            Map<String, Object> details = walletDetails(userId, normalizedAsset, null);
            log.warn("event=futures.wallet.balance.fail userId={} asset={} reasonCode=network_timeout errClass={} errMsg=\"{}\" friendlyStep=binance_no_respondio_a_tiempo",
                    userId, normalizedAsset, ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Timeout/red consultando saldo Futures", ex, details);
        } catch (EngineException ex) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("userId", userId);
            details.put("asset", normalizedAsset);
            details.put("errCode", ex.getErrorCode() == null ? "" : ex.getErrorCode().name());
            details.put("errMsg", safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Respuesta inválida consultando saldo Futures", ex, details);
        } catch (RestClientException | IllegalStateException | IllegalArgumentException ex) {
            Map<String, Object> details = walletDetails(userId, normalizedAsset, null);
            log.warn("event=futures.wallet.balance.fail userId={} asset={} reasonCode=client_error errClass={} errMsg=\"{}\" friendlyStep=fallo_el_cliente_http",
                    userId, normalizedAsset, ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Error cliente consultando saldo Futures", ex, details);
        }
    }

    @Override
    public FuturesConvertToBnbClientResponse convertStableAssetToBnb(UserDetailDto userDetail,
                                                                     FuturesCapitalAsset fromAsset,
                                                                     BigDecimal amount) {
        validateUserCredentials(userDetail);
        if (fromAsset == null) {
            throw new SkipExecutionException("capital_asset_missing", "Moneda de capital requerida para convertir", null);
        }
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new SkipExecutionException("conversion_amount_invalid", "La cantidad a convertir debe ser mayor a cero", LogFmt.kv("amount", amount));
        }

        String userId = userId(userDetail);
        FuturesConvertToBnbClientRequest request = new FuturesConvertToBnbClientRequest(fromAsset.name(), amount);

        try {
            ApiResponse<FuturesConvertToBnbClientResponse> response = binanceClient.convertToBnb(
                    userDetail.getUserApiKey().getApiKey(),
                    userDetail.getUserApiKey().getApiSecret(),
                    null,
                    userId,
                    null,
                    traceId(),
                    request
            );
            FuturesConvertToBnbClientResponse result = unwrap(response, "futures.wallet.convert_to_bnb");
            log.info("event=futures.wallet.convert_to_bnb.ok userId={} fromAsset={} amount={} success={} pending={} orderStatus={} orderId={} friendlyStep=binance_respondio_la_conversion",
                    userId, fromAsset, amount, result.isSuccess(), result.isPending(), safeNull(result.getOrderStatus()), safeNull(result.getOrderId()));
            return result;
        } catch (RestClientResponseException ex) {
            BinanceHttpError err = parseBinanceHttpError(ex);
            Map<String, Object> details = walletDetails(userId, fromAsset.name(), err);
            details.put("amount", amount.toPlainString());
            log.warn("event=futures.wallet.convert_to_bnb.fail userId={} fromAsset={} amount={} httpStatus={} errorCode={} binanceCode={} binanceMsg=\"{}\" friendlyStep=no_pude_convertir_a_bnb",
                    userId, fromAsset, amount, err.httpStatus(), safeNull(err.errorCode()), safeNull(err.binanceCode()), safeLog(err.binanceMsg()));
            throw new FuturesWalletClientException("No se pudo convertir " + fromAsset + " a BNB", ex, details);
        } catch (ResourceAccessException ex) {
            Map<String, Object> details = walletDetails(userId, fromAsset.name(), null);
            details.put("amount", amount.toPlainString());
            log.warn("event=futures.wallet.convert_to_bnb.fail userId={} fromAsset={} amount={} reasonCode=network_timeout errClass={} errMsg=\"{}\" friendlyStep=binance_no_respondio_a_tiempo",
                    userId, fromAsset, amount, ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Timeout/red convirtiendo saldo Futures a BNB", ex, details);
        } catch (EngineException ex) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("userId", userId);
            details.put("fromAsset", fromAsset.name());
            details.put("amount", amount.toPlainString());
            details.put("errCode", ex.getErrorCode() == null ? "" : ex.getErrorCode().name());
            details.put("errMsg", safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Respuesta inválida convirtiendo saldo Futures a BNB", ex, details);
        } catch (RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            Map<String, Object> details = walletDetails(userId, fromAsset.name(), null);
            details.put("amount", amount.toPlainString());
            log.warn("event=futures.wallet.convert_to_bnb.fail userId={} fromAsset={} amount={} reasonCode=client_error errClass={} errMsg=\"{}\" friendlyStep=fallo_el_cliente_http",
                    userId, fromAsset, amount, ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            throw new FuturesWalletClientException("Error cliente convirtiendo saldo Futures a BNB", ex, details);
        }
    }

    private void validateUserCredentials(UserDetailDto userDetail) {
        if (userDetail == null || userDetail.getUser() == null || userDetail.getUser().getId() == null) {
            throw new SkipExecutionException("user_detail_missing", "Detalle de usuario requerido", null);
        }
        if (userDetail.getUserApiKey() == null) {
            throw new SkipExecutionException("api_key_missing", "Usuario sin API key configurada", LogFmt.kv("userId", userId(userDetail)));
        }
        if (userDetail.getUserApiKey().getApiKey() == null || userDetail.getUserApiKey().getApiKey().isBlank()) {
            throw new SkipExecutionException("api_key_missing", "API key requerida", LogFmt.kv("userId", userId(userDetail)));
        }
        if (userDetail.getUserApiKey().getApiSecret() == null || userDetail.getUserApiKey().getApiSecret().isBlank()) {
            throw new SkipExecutionException("api_secret_missing", "API secret requerida", LogFmt.kv("userId", userId(userDetail)));
        }
    }

    private String normalizeWalletAsset(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new SkipExecutionException("wallet_asset_missing", "Moneda requerida para consultar saldo", null);
        }
        String asset = raw.trim().toUpperCase(Locale.ROOT);
        if (FuturesCapitalAsset.isAllowedStable(asset) || ASSET_BNB.equals(asset)) {
            return asset;
        }
        throw new SkipExecutionException("wallet_asset_invalid", "Solo se permite consultar USDT, USDC o BNB en este proceso", LogFmt.kv("asset", raw));
    }

    private <T> T unwrap(ApiResponse<T> response, String operation) {
        if (response == null) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta nula de ms-binance (" + operation + ")");
        }
        T data = response.getData();
        int code = response.getStatusCode();
        if (data != null && (code == 0 || (code >= 200 && code < 300))) {
            return data;
        }
        String message = response.getMessage() == null || response.getMessage().isBlank()
                ? "sin mensaje"
                : response.getMessage();
        if (code == 429) {
            throw new EngineException(ErrorCode.BINANCE_RATE_LIMIT, "Rate limit ms-binance (" + operation + "): " + message);
        }
        throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida ms-binance (" + operation + ") code=" + code + " msg=" + safeLog(message));
    }

    private BinanceHttpError parseBinanceHttpError(RestClientResponseException ex) {
        try {
            JsonNode root = objectMapper.readTree(ex.getResponseBodyAsString());
            JsonNode data = root.path("data");
            JsonNode details = data.path("details");
            return new BinanceHttpError(
                    ex.getStatusCode().value(),
                    text(data, "errorCode"),
                    text(details, "binanceCode"),
                    text(details, "binanceMsg"),
                    text(root, "traceId"),
                    text(root, "path")
            );
        } catch (JsonProcessingException parseEx) {
            return new BinanceHttpError(ex.getStatusCode().value(), "HTTP_ERROR", null, ex.getStatusText(), null, null);
        }
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? null : value.asText();
    }

    private Map<String, Object> walletDetails(String userId, String asset, BinanceHttpError err) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("userId", safeNull(userId));
        details.put("asset", safeNull(asset));
        if (err != null) {
            details.put("httpStatus", Integer.toString(err.httpStatus()));
            details.put("errorCode", safeNull(err.errorCode()));
            details.put("binanceCode", safeNull(err.binanceCode()));
            details.put("binanceMsg", safeLog(err.binanceMsg()));
            details.put("traceId", safeNull(err.traceId()));
            details.put("path", safeNull(err.path()));
        }
        return details;
    }

    private String userId(UserDetailDto userDetail) {
        UUID id = userDetail == null || userDetail.getUser() == null ? null : userDetail.getUser().getId();
        return id == null ? "" : id.toString();
    }

    private String traceId() {
        String traceId = MDC.get("traceId");
        return traceId == null || traceId.isBlank() ? null : traceId;
    }

    private String safeLog(String value) {
        if (value == null) {
            return "";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private String safeNull(String value) {
        return value == null ? "" : value;
    }

    private record BinanceHttpError(
            int httpStatus,
            String errorCode,
            String binanceCode,
            String binanceMsg,
            String traceId,
            String path
    ) {
    }
}

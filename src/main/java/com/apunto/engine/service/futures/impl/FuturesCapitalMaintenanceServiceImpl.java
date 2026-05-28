package com.apunto.engine.service.futures.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.futures.BinanceFuturesWalletService;
import com.apunto.engine.service.futures.FuturesAssetAmountParser;
import com.apunto.engine.service.futures.FuturesBnbConversionDecision;
import com.apunto.engine.service.futures.FuturesBnbConversionPolicy;
import com.apunto.engine.service.futures.FuturesBnbPriceService;
import com.apunto.engine.service.futures.FuturesCapitalMaintenanceService;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class FuturesCapitalMaintenanceServiceImpl implements FuturesCapitalMaintenanceService {

    private static final String ASSET_BNB = "BNB";
    private static final int CAPITAL_SCALE = 0;

    private final UserDetailService userDetailService;
    private final BinanceFuturesWalletService walletService;
    private final FuturesBnbPriceService bnbPriceService;
    private final FuturesBnbConversionPolicy conversionPolicy;
    private final DetailUserRepository detailUserRepository;
    private final UserDetailCachedService userDetailCachedService;

    @Value("${futures.capital-maintenance.min-available:50}")
    private BigDecimal minimumAvailableToConvert;

    @Value("${futures.capital-maintenance.bnb-min-ratio:0.03}")
    private BigDecimal minimumBnbRatio;

    @Value("${futures.capital-maintenance.conversion-ratio:0.10}")
    private BigDecimal conversionRatio;

    @Override
    public void maintainAllActiveUsersCapital() {
        long startedNs = System.nanoTime();
        List<UserDetailDto> users = userDetailService.findAllActive();
        int processed = 0;
        int converted = 0;
        int skipped = 0;
        int failed = 0;

        log.info("event=futures.capital_maintenance.start users={} minAvailable={} bnbMinRatio={} conversionRatio={} friendlyStep=voy_a_revisar_saldos_y_bnb_de_los_usuarios_activos",
                users.size(), minimumAvailableToConvert, minimumBnbRatio, conversionRatio);

        for (UserDetailDto user : users) {
            try {
                UserProcessResult result = processOneUser(user);
                processed++;
                if (result.converted()) {
                    converted++;
                } else {
                    skipped++;
                }
            } catch (SkipExecutionException ex) {
                skipped++;
                log.info("event=futures.capital_maintenance.user.skip userId={} reasonCode={} reason=\"{}\" details=\"{}\" friendlyStep=este_usuario_no_cumple_una_regla_y_lo_salto_sin_romper_el_proceso",
                        userId(user), ex.getReasonCode(), safeLog(ex.getReason()), safeLog(ex.getDetails()));
            } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
                failed++;
                log.warn("event=futures.capital_maintenance.user.fail userId={} errClass={} errMsg=\"{}\" friendlyStep=fallo_un_usuario_pero_sigo_con_los_demas",
                        userId(user), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            }
        }

        log.info("event=futures.capital_maintenance.done users={} processed={} converted={} skipped={} failed={} elapsedMs={} friendlyStep=termine_la_revision_de_capital_y_bnb",
                users.size(), processed, converted, skipped, failed, elapsedMs(startedNs));
    }

    private UserProcessResult processOneUser(UserDetailDto userDetail) {
        validateUser(userDetail);

        String userId = userId(userDetail);
        UUID detailId = userDetail.getDetail().getId();
        FuturesCapitalAsset capitalAsset = FuturesCapitalAsset.fromNullable(userDetail.getDetail().getCapitalAsset());

        FuturesAssetBalanceClientResponse baseBalance = walletService.getAssetBalance(userDetail, capitalAsset.name());
        BigDecimal available = FuturesAssetAmountParser.positiveOrZero(baseBalance.getAvailableBalance());
        persistCapital(userDetail, detailId, capitalAsset, available, "balance_read");

        FuturesBnbConversionDecision decision;
        BigDecimal bnbAmount = BigDecimal.ZERO;
        BigDecimal safeMinimumAvailable = positiveConfig(minimumAvailableToConvert);
        if (available.compareTo(safeMinimumAvailable) < 0) {
            decision = conversionPolicy.decide(
                    capitalAsset,
                    available,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    minimumAvailableToConvert,
                    minimumBnbRatio,
                    conversionRatio
            );
            log.info("event=futures.capital_maintenance.low_balance.skip_extra_calls userId={} asset={} available={} minimumAvailable={} friendlyStep=el_saldo_es_menor_a_50_y_no_pregunto_bnb_ni_precio_para_ser_mas_rapido",
                    userId, capitalAsset, available.toPlainString(), safeMinimumAvailable.toPlainString());
        } else {
            FuturesAssetBalanceClientResponse bnbBalance = walletService.getAssetBalance(userDetail, ASSET_BNB);
            bnbAmount = preferredBnbAmount(bnbBalance);
            BigDecimal bnbPrice = bnbPriceService.getBnbPrice(capitalAsset);
            decision = conversionPolicy.decide(
                    capitalAsset,
                    available,
                    bnbAmount,
                    bnbPrice,
                    minimumAvailableToConvert,
                    minimumBnbRatio,
                    conversionRatio
            );
        }

        boolean converted = false;
        BigDecimal finalAvailable = available;
        if (decision.shouldConvert()) {
            log.info("event=futures.capital_maintenance.convert.start userId={} asset={} available={} bnbBalance={} bnbValue={} bnbMinimumValue={} amountToConvert={} friendlyStep=el_bnb_es_muy_bajito_y_convierto_el_10_por_ciento",
                    userId,
                    capitalAsset,
                    available.toPlainString(),
                    bnbAmount.toPlainString(),
                    decision.bnbValue().toPlainString(),
                    decision.bnbMinimumValue().toPlainString(),
                    decision.conversionAmount().toPlainString());

            FuturesConvertToBnbClientResponse conversion = walletService.convertStableAssetToBnb(
                    userDetail,
                    capitalAsset,
                    decision.conversionAmount()
            );
            converted = conversion.isSuccess() || conversion.isPending();
            finalAvailable = refreshAvailableAfterConversion(userDetail, capitalAsset, available, converted);

            log.info("event=futures.capital_maintenance.convert.done userId={} asset={} converted={} success={} pending={} orderStatus={} orderId={} finalAvailable={} friendlyStep=la_conversion_termino_y_actualizo_el_capital",
                    userId,
                    capitalAsset,
                    converted,
                    conversion.isSuccess(),
                    conversion.isPending(),
                    safeLog(conversion.getOrderStatus()),
                    safeLog(conversion.getOrderId()),
                    finalAvailable.toPlainString());
        } else {
            log.info("event=futures.capital_maintenance.convert.skip userId={} asset={} reasonCode={} available={} bnbBalance={} bnbValue={} bnbMinimumValue={} friendlyStep=no_convierto_porque_la_regla_dice_que_no_hace_falta",
                    userId,
                    capitalAsset,
                    decision.reasonCode(),
                    available.toPlainString(),
                    bnbAmount.toPlainString(),
                    decision.bnbValue().toPlainString(),
                    decision.bnbMinimumValue().toPlainString());
        }

        persistCapital(userDetail, detailId, capitalAsset, finalAvailable, "final_state");

        return new UserProcessResult(converted);
    }


    private void persistCapital(UserDetailDto userDetail,
                                UUID detailId,
                                FuturesCapitalAsset capitalAsset,
                                BigDecimal availableBalance,
                                String phase) {
        int capital = toCapitalInteger(availableBalance);
        int updated = detailUserRepository.updateCapitalById(detailId, capital);
        userDetailCachedService.updateRuntimeCapital(userDetail.getUser().getId(), capital, capitalAsset.name());
        log.info("event=futures.capital_maintenance.capital.updated phase={} userId={} detailId={} asset={} capital={} availableBalance={} rows={} friendlyStep=guarde_el_capital_actual_para_que_copytrading_lo_use_rapido_en_memoria",
                phase, userId(userDetail), detailId, capitalAsset, capital, availableBalance.toPlainString(), updated);
    }

    private BigDecimal refreshAvailableAfterConversion(UserDetailDto userDetail,
                                                       FuturesCapitalAsset capitalAsset,
                                                       BigDecimal fallbackAvailable,
                                                       boolean converted) {
        if (!converted) {
            return fallbackAvailable;
        }
        try {
            FuturesAssetBalanceClientResponse refreshed = walletService.getAssetBalance(userDetail, capitalAsset.name());
            BigDecimal refreshedAvailable = FuturesAssetAmountParser.positiveOrZero(refreshed.getAvailableBalance());
            return refreshedAvailable.compareTo(BigDecimal.ZERO) >= 0 ? refreshedAvailable : fallbackAvailable;
        } catch (EngineException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=futures.capital_maintenance.balance.refresh_after_convert.fail userId={} asset={} errClass={} errMsg=\"{}\" friendlyStep=no_pude_releer_el_saldo_y_uso_el_anterior",
                    userId(userDetail), capitalAsset, ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            return fallbackAvailable;
        }
    }

    private BigDecimal preferredBnbAmount(FuturesAssetBalanceClientResponse bnbBalance) {
        if (bnbBalance == null) {
            return BigDecimal.ZERO;
        }
        BigDecimal wallet = FuturesAssetAmountParser.positiveOrZero(bnbBalance.getWalletBalance());
        if (wallet.compareTo(BigDecimal.ZERO) > 0) {
            return wallet;
        }
        return FuturesAssetAmountParser.positiveOrZero(bnbBalance.getAvailableBalance());
    }

    private void validateUser(UserDetailDto userDetail) {
        if (userDetail == null || userDetail.getUser() == null || userDetail.getUser().getId() == null) {
            throw new SkipExecutionException("user_detail_missing", "Detalle de usuario requerido", null);
        }
        DetailUserEntity detail = userDetail.getDetail();
        if (detail == null || detail.getId() == null) {
            throw new SkipExecutionException("detail_user_missing", "Detalle detail_user requerido", com.apunto.engine.shared.util.LogFmt.kv("userId", userId(userDetail)));
        }
        if (!detail.isUserActive()) {
            throw new SkipExecutionException("user_inactive", "Usuario inactivo", com.apunto.engine.shared.util.LogFmt.kv("userId", userId(userDetail)));
        }
        if (!detail.isApiKeyBinar()) {
            throw new SkipExecutionException("api_key_inactive", "API key Binance inactiva", com.apunto.engine.shared.util.LogFmt.kv("userId", userId(userDetail)));
        }
    }

    private BigDecimal positiveConfig(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }
        return value;
    }

    private int toCapitalInteger(BigDecimal available) {
        if (available == null || available.compareTo(BigDecimal.ZERO) <= 0) {
            return 0;
        }
        BigDecimal capped = available.min(BigDecimal.valueOf(Integer.MAX_VALUE));
        return capped.setScale(CAPITAL_SCALE, RoundingMode.DOWN).intValue();
    }

    private String userId(UserDetailDto userDetail) {
        if (userDetail == null || userDetail.getUser() == null || userDetail.getUser().getId() == null) {
            return "";
        }
        return userDetail.getUser().getId().toString();
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null) {
            return "";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private record UserProcessResult(boolean converted) {
    }
}

package com.apunto.copytarget;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure, deterministic proportional portfolio calculator.
 */
public final class TargetPortfolioCalculator {

    private static final Comparator<Candidate> SELECTION_ORDER = Comparator
            .comparing(Candidate::exposure, Comparator.reverseOrder())
            .thenComparing(Candidate::liquidity, Comparator.reverseOrder())
            .thenComparing(Candidate::roundingError)
            .thenComparing(candidate -> candidate.position().targetSymbol())
            .thenComparing(candidate -> candidate.position().sourceLegId());

    public TargetPortfolioResult calculate(TargetPortfolioRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }

        Map<String, BinanceSymbolFilter> filters = indexFilters(request.filters());
        Map<String, ExistingTargetPosition> actualExisting = aggregateExisting(request.existingPositions());
        Map<String, ExistingTargetPosition> managedExisting = aggregateExisting(request.managedExistingPositions());
        Map<String, ExistingTargetPosition> existing = aggregateExisting(request.portfolioExistingPositions());
        DecisionCode equityFailure = equityFailure(request);
        DecisionCode targetPositionFailure = targetPositionFailure(
                request, actualExisting, managedExisting, existing);

        List<TargetLegDecision> omitted = new ArrayList<>();
        List<Candidate> candidates = new ArrayList<>();
        Set<String> sourceLegIds = new HashSet<>();

        for (SourcePosition position : request.sourcePositions()) {
            if (!sourceLegIds.add(position.sourceLegId())) {
                throw new IllegalArgumentException("duplicate sourceLegId: " + position.sourceLegId());
            }
            if (equityFailure != null) {
                omitted.add(omitted(position, equityFailure, "source equity is not entry-eligible"));
                continue;
            }
            CandidateResolution resolution = resolveCandidate(request, position, filters);
            if (resolution.omitted() != null) {
                omitted.add(resolution.omitted());
            } else {
                candidates.add(resolution.candidate());
            }
        }

        Set<String> collisionKeys = targetCollisionKeys(candidates);
        Set<String> collisionProtectedPositionKeys = new HashSet<>();
        if (!collisionKeys.isEmpty()) {
            List<Candidate> unambiguous = new ArrayList<>();
            for (Candidate candidate : candidates) {
                SourcePosition position = candidate.position();
                String positionKey = key(position.targetSymbol(), position.side());
                if (collisionKeys.contains(positionKey)) {
                    omitted.add(omitted(position, DecisionCode.BLOCKED_TARGET_SYMBOL_COLLISION,
                            "multiple source legs resolve to the same targetSymbol and side"));
                    collisionProtectedPositionKeys.add(positionKey);
                    collisionProtectedPositionKeys.add(key(position.targetSymbol(), opposite(position.side())));
                } else {
                    unambiguous.add(candidate);
                }
            }
            candidates = unambiguous;
        }
        boolean entrySizingAllowed = equityFailure == null
                && targetPositionFailure == null
                && collisionKeys.isEmpty();

        candidates.sort(SELECTION_ORDER);
        BigDecimal availableTargetMargin = availableTargetMargin(request);
        candidates = applyUserPositionLimit(request, candidates, omitted, existing, availableTargetMargin);

        BigDecimal totalRequiredMargin = candidates.stream()
                .map(Candidate::rawTargetMargin)
                .reduce(DecimalSupport.ZERO, BigDecimal::add);
        BigDecimal scaleFactor = scaleFactor(availableTargetMargin, totalRequiredMargin);

        List<TargetLegDecision> selected = new ArrayList<>();
        Set<String> desiredPositionKeys = new HashSet<>(collisionProtectedPositionKeys);
        BigDecimal selectedSourceRawNotional = DecimalSupport.ZERO;
        BigDecimal finalTargetNotional = DecimalSupport.ZERO;
        int selectedSourceMovements = 0;

        for (Candidate candidate : candidates) {
            TargetLegDecision decision = finalizeCandidate(candidate, scaleFactor, existing);
            if (decision.selected()) {
                selected.add(decision);
                desiredPositionKeys.add(key(decision.targetSymbol(), decision.side()));
                selectedSourceMovements++;
                selectedSourceRawNotional = selectedSourceRawNotional.add(candidate.rawTargetNotional());
                finalTargetNotional = finalTargetNotional.add(decision.targetNotionalUsd());
            } else {
                omitted.add(decision);
            }
        }

        boolean authoritativeFlatOrSizedPortfolio = equityFailure == null
                || request.sourcePositions().isEmpty();
        if (authoritativeFlatOrSizedPortfolio) {
            addRequiredCloses(existing, desiredPositionKeys, selected, filters);
        }

        BigDecimal totalRawNotional = request.sourcePositions().stream()
                .map(position -> rawTargetNotional(request, position))
                .reduce(DecimalSupport.ZERO, BigDecimal::add);
        boolean equityBlockedWithSourcePositions = equityFailure != null
                && !request.sourcePositions().isEmpty();
        BigDecimal movementCoverage = equityBlockedWithSourcePositions
                ? DecimalSupport.ZERO
                : request.sourcePositions().isEmpty()
                ? DecimalSupport.ONE
                : DecimalSupport.ratio(BigDecimal.valueOf(selectedSourceMovements),
                BigDecimal.valueOf(request.sourcePositions().size()));
        BigDecimal notionalCoverage = equityBlockedWithSourcePositions
                ? DecimalSupport.ZERO
                : totalRawNotional.compareTo(DecimalSupport.ZERO) <= 0
                ? DecimalSupport.ONE
                : DecimalSupport.ratio(finalTargetNotional, totalRawNotional);
        BigDecimal exposureCoverage = notionalCoverage;
        BigDecimal finalMargin = selected.stream()
                .map(TargetLegDecision::targetMarginUsd)
                .reduce(DecimalSupport.ZERO, BigDecimal::add);

        DecisionCode portfolioCode;
        if (equityFailure != null && !request.sourcePositions().isEmpty()) {
            portfolioCode = equityFailure;
        } else if (!collisionKeys.isEmpty()) {
            portfolioCode = DecisionCode.BLOCKED_TARGET_SYMBOL_COLLISION;
        } else if (targetPositionFailure != null) {
            portfolioCode = targetPositionFailure;
        } else if (!request.sourcePositions().isEmpty()
                && availableTargetMargin.compareTo(DecimalSupport.ZERO) <= 0) {
            portfolioCode = DecisionCode.BLOCKED_INSUFFICIENT_MARGIN;
        } else {
            portfolioCode = DecisionCode.TARGET_CALCULATED;
        }

        return new TargetPortfolioResult(
                portfolioCode,
                entrySizingAllowed,
                scaleFactor,
                availableTargetMargin,
                totalRawNotional,
                finalTargetNotional,
                finalMargin,
                movementCoverage,
                notionalCoverage,
                exposureCoverage,
                selected,
                omitted,
                request.versions(),
                request.sourceSnapshotVersion(),
                request.calculatedAt()
        );
    }

    private Set<String> targetCollisionKeys(List<Candidate> candidates) {
        Map<String, Integer> counts = new HashMap<>();
        for (Candidate candidate : candidates) {
            SourcePosition position = candidate.position();
            counts.merge(key(position.targetSymbol(), position.side()), 1, Integer::sum);
        }
        Set<String> collisions = new HashSet<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() > 1) {
                collisions.add(entry.getKey());
            }
        }
        return collisions;
    }

    private List<Candidate> applyUserPositionLimit(TargetPortfolioRequest request,
                                                   List<Candidate> orderedCandidates,
                                                   List<TargetLegDecision> omitted,
                                                   Map<String, ExistingTargetPosition> attributedExisting,
                                                   BigDecimal availableTargetMargin) {
        Integer maximumPositions = request.userMaxConcurrentPositions();
        if (maximumPositions == null) {
            return orderedCandidates;
        }

        List<Candidate> selected = new ArrayList<>();
        List<Candidate> newCandidates = new ArrayList<>();
        for (Candidate candidate : orderedCandidates) {
            if (replacesAttributedPosition(candidate, attributedExisting)) {
                selected.add(candidate);
            } else {
                newCandidates.add(candidate);
            }
        }

        long managedOpenPositions = request.managedExistingPositions().stream()
                .filter(position -> position.quantity().compareTo(DecimalSupport.ZERO) > 0)
                .count();
        int remainingNewSlots = Math.max(0, maximumPositions - Math.toIntExact(managedOpenPositions));
        int acceptedNewPositions = 0;

        for (Candidate candidate : newCandidates) {
            if (acceptedNewPositions >= remainingNewSlots) {
                omitted.add(omitted(candidate.position(), DecisionCode.SKIPPED_USER_POSITION_LIMIT,
                        "account-wide userMaxConcurrentPositions has no slot for a new position"));
                continue;
            }

            List<Candidate> tentative = new ArrayList<>(selected);
            tentative.add(candidate);
            BigDecimal tentativeScale = scaleFactor(availableTargetMargin, tentative.stream()
                    .map(Candidate::rawTargetMargin)
                    .reduce(DecimalSupport.ZERO, BigDecimal::add));
            TargetLegDecision candidateDecision = finalizeCandidate(candidate, tentativeScale, attributedExisting);
            boolean prioritizedCandidatesRemainExecutable = selected.stream()
                    .map(existingCandidate -> finalizeCandidate(existingCandidate, tentativeScale, attributedExisting))
                    .allMatch(TargetLegDecision::selected);

            if (candidateDecision.selected() && prioritizedCandidatesRemainExecutable) {
                selected.add(candidate);
                acceptedNewPositions++;
            } else if (!candidateDecision.selected()) {
                omitted.add(candidateDecision);
            } else {
                omitted.add(omitted(candidate.position(), DecisionCode.SKIPPED_NOT_SELECTED_BY_USER_POLICY,
                        "lower-ranked candidate would make a prioritized target non-executable"));
            }
        }
        return selected;
    }

    private boolean replacesAttributedPosition(Candidate candidate,
                                                Map<String, ExistingTargetPosition> attributedExisting) {
        SourcePosition position = candidate.position();
        return attributedExisting.containsKey(key(position.targetSymbol(), position.side()))
                || attributedExisting.containsKey(key(position.targetSymbol(), opposite(position.side())));
    }

    private BigDecimal scaleFactor(BigDecimal availableTargetMargin, BigDecimal totalRequiredMargin) {
        return totalRequiredMargin.compareTo(DecimalSupport.ZERO) <= 0
                ? DecimalSupport.ONE
                : DecimalSupport.min(DecimalSupport.ONE,
                DecimalSupport.divideDown(availableTargetMargin, totalRequiredMargin));
    }

    private CandidateResolution resolveCandidate(TargetPortfolioRequest request,
                                                 SourcePosition position,
                                                 Map<String, BinanceSymbolFilter> filters) {
        if (position.targetSymbol() == null || position.targetSymbol().isBlank()) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.SKIPPED_NO_BINANCE_ALIAS, "target symbol alias is missing"));
        }
        BinanceSymbolFilter filter = filters.get(position.targetSymbol());
        if (filter == null || !filter.trading() || !filter.quoteAsset().equals(request.quoteAsset())) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.SKIPPED_SYMBOL_NOT_SUPPORTED,
                    "symbol is missing, suspended or uses a different quote asset"));
        }
        if (filter.stepSize().compareTo(DecimalSupport.ZERO) <= 0) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.REJECTED_BY_BINANCE_FILTER, "stepSize must be positive"));
        }
        if (request.targetLeverage().compareTo(DecimalSupport.ZERO) <= 0
                || filter.maximumLeverage().compareTo(DecimalSupport.ZERO) <= 0
                || request.targetLeverage().compareTo(filter.maximumLeverage()) > 0) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.BLOCKED_LEVERAGE_LIMIT, "target leverage exceeds the symbol contract"));
        }
        if (position.notionalUsd().compareTo(DecimalSupport.ZERO) == 0) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.SKIPPED_ALREADY_AT_TARGET, "source position has zero notional"));
        }

        BigDecimal price = positivePrice(position);
        if (price.compareTo(DecimalSupport.ZERO) <= 0) {
            return CandidateResolution.omitted(omitted(position,
                    DecisionCode.REJECTED_BY_BINANCE_FILTER, "source price is not positive"));
        }
        BigDecimal exposure = DecimalSupport.divideDown(
                position.marginUsedUsd().abs(), request.sourceAccountEquityUsd());
        BigDecimal rawTargetMargin = DecimalSupport.normalize(
                request.targetAllocatedCapitalUsd().multiply(exposure));
        BigDecimal rawTargetNotional = DecimalSupport.normalize(
                rawTargetMargin.multiply(request.targetLeverage()));
        BigDecimal preliminaryRawQty = DecimalSupport.divideDown(rawTargetNotional, price);
        BigDecimal preliminaryRoundedQty = DecimalSupport.floorToStep(preliminaryRawQty, filter.stepSize());
        BigDecimal roundingError = DecimalSupport.normalize(
                preliminaryRawQty.subtract(preliminaryRoundedQty).max(DecimalSupport.ZERO));
        BigDecimal liquidity = filter.liquidityScore().max(position.liquidityScore());
        return CandidateResolution.candidate(new Candidate(
                position,
                filter,
                price,
                exposure,
                rawTargetNotional,
                rawTargetMargin,
                request.targetLeverage(),
                liquidity,
                roundingError
        ));
    }

    private TargetLegDecision finalizeCandidate(Candidate candidate,
                                                BigDecimal scaleFactor,
                                                Map<String, ExistingTargetPosition> existing) {
        SourcePosition position = candidate.position();
        BinanceSymbolFilter filter = candidate.filter();
        BigDecimal scaledNotional = DecimalSupport.normalize(
                candidate.rawTargetNotional().multiply(scaleFactor));
        if (scaledNotional.compareTo(DecimalSupport.ZERO) <= 0) {
            return omitted(candidate, DecisionCode.SKIPPED_CAPITAL_EXHAUSTED,
                    "portfolio has no target margin capacity", DecimalSupport.ZERO, DecimalSupport.ZERO);
        }

        BigDecimal rawQuantity = DecimalSupport.divideDown(scaledNotional, candidate.price());
        BigDecimal roundedQuantity = DecimalSupport.floorToStep(rawQuantity, filter.stepSize());
        if (rawQuantity.compareTo(filter.minQty()) < 0 || roundedQuantity.compareTo(filter.minQty()) < 0) {
            return omitted(candidate, DecisionCode.SKIPPED_BELOW_MIN_QTY,
                    "quantity is below Binance minQty", rawQuantity, roundedQuantity);
        }
        if (roundedQuantity.compareTo(DecimalSupport.ZERO) <= 0) {
            return omitted(candidate, DecisionCode.SKIPPED_ROUNDED_TO_ZERO,
                    "quantity rounded to zero", rawQuantity, roundedQuantity);
        }
        if (filter.maxQty().compareTo(DecimalSupport.ZERO) > 0
                && roundedQuantity.compareTo(filter.maxQty()) > 0) {
            return omitted(candidate, DecisionCode.REJECTED_BY_BINANCE_FILTER,
                    "quantity exceeds Binance maxQty", rawQuantity, roundedQuantity);
        }
        BigDecimal roundedNotional = DecimalSupport.normalize(roundedQuantity.multiply(candidate.price()));
        if (roundedNotional.compareTo(filter.minNotional()) < 0) {
            return omitted(candidate, DecisionCode.SKIPPED_BELOW_MIN_NOTIONAL,
                    "rounded target is below Binance minNotional", rawQuantity, roundedQuantity);
        }

        String positionKey = key(position.targetSymbol(), position.side());
        ExistingTargetPosition current = existing.get(positionKey);
        BigDecimal existingQuantity = current == null ? DecimalSupport.ZERO : current.quantity();
        BigDecimal rawDelta = DecimalSupport.normalize(roundedQuantity.subtract(existingQuantity));
        BigDecimal executableDeltaMagnitude = DecimalSupport.floorToStep(rawDelta.abs(), filter.stepSize());
        BigDecimal deltaQuantity = rawDelta.signum() < 0
                ? executableDeltaMagnitude.negate()
                : executableDeltaMagnitude;
        DeltaAction action;
        DecisionCode decisionCode = DecisionCode.TARGET_CALCULATED;
        if (executableDeltaMagnitude.compareTo(DecimalSupport.ZERO) <= 0) {
            action = DeltaAction.NONE;
            decisionCode = rawDelta.compareTo(DecimalSupport.ZERO) == 0
                    ? DecisionCode.SKIPPED_ALREADY_AT_TARGET
                    : DecisionCode.SKIPPED_DELTA_TOO_SMALL;
        } else if (rawDelta.signum() > 0) {
            action = existingQuantity.compareTo(DecimalSupport.ZERO) == 0
                    ? DeltaAction.OPEN
                    : DeltaAction.INCREASE;
        } else {
            action = roundedQuantity.compareTo(DecimalSupport.ZERO) == 0
                    ? DeltaAction.CLOSE
                    : DeltaAction.REDUCE;
        }

        boolean waitsForOppositeClose = existing.containsKey(
                key(position.targetSymbol(), opposite(position.side())));
        BigDecimal margin = DecimalSupport.divideDown(roundedNotional, candidate.targetLeverage());
        BigDecimal roundingLoss = DecimalSupport.normalize(
                scaledNotional.subtract(roundedNotional).max(DecimalSupport.ZERO));
        return new TargetLegDecision(
                position.sourceLegId(),
                position.sourceSymbol(),
                position.targetSymbol(),
                position.side(),
                true,
                decisionCode,
                "",
                candidate.exposure(),
                candidate.rawTargetNotional(),
                roundedNotional,
                margin,
                rawQuantity,
                roundedQuantity,
                roundedQuantity,
                existingQuantity,
                deltaQuantity,
                action,
                roundingLoss,
                candidate.liquidity(),
                waitsForOppositeClose
        );
    }

    private void addRequiredCloses(Map<String, ExistingTargetPosition> existing,
                                   Set<String> desiredPositionKeys,
                                   List<TargetLegDecision> selected,
                                   Map<String, BinanceSymbolFilter> filters) {
        for (ExistingTargetPosition position : existing.values()) {
            if (desiredPositionKeys.contains(position.key())
                    || position.quantity().compareTo(DecimalSupport.ZERO) <= 0) {
                continue;
            }
            boolean oppositeDesired = desiredPositionKeys.contains(key(position.symbol(), opposite(position.side())));
            BinanceSymbolFilter filter = filters.get(position.symbol());
            BigDecimal closeQuantity = filter == null
                    ? position.quantity()
                    : DecimalSupport.floorToStep(position.quantity(), filter.stepSize());
            selected.add(new TargetLegDecision(
                    "existing:" + position.key(),
                    "",
                    position.symbol(),
                    position.side(),
                    true,
                    DecisionCode.TARGET_CALCULATED,
                    oppositeDesired ? "opposite position must close before opening" : "source target is flat",
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    position.quantity(),
                    closeQuantity.negate(),
                    oppositeDesired ? DeltaAction.FLIP_CLOSE : DeltaAction.CLOSE,
                    DecimalSupport.ZERO,
                    DecimalSupport.ZERO,
                    false
            ));
        }
    }

    private DecisionCode equityFailure(TargetPortfolioRequest request) {
        BigDecimal equity = request.sourceAccountEquityUsd();
        if (equity == null) {
            return DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING;
        }
        if (equity.compareTo(DecimalSupport.ZERO) <= 0) {
            return DecisionCode.BLOCKED_SOURCE_EQUITY_INVALID;
        }
        if (request.equityObservedAt() == null || request.equitySource() == null
                || request.equitySource().isBlank()) {
            return DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING;
        }
        Duration age = Duration.between(request.equityObservedAt(), request.calculatedAt());
        if (age.isNegative()) {
            return DecisionCode.BLOCKED_SOURCE_EQUITY_INVALID;
        }
        if (age.compareTo(request.maximumEquityAge()) > 0) {
            return DecisionCode.BLOCKED_SOURCE_EQUITY_STALE;
        }
        boolean snapshotMismatch = request.sourcePositions().stream()
                .anyMatch(position -> position.snapshotVersion() != request.sourceSnapshotVersion());
        if (snapshotMismatch) {
            return DecisionCode.BLOCKED_SOURCE_SNAPSHOT_MISMATCH;
        }
        return null;
    }

    private DecisionCode targetPositionFailure(
            TargetPortfolioRequest request,
            Map<String, ExistingTargetPosition> actual,
            Map<String, ExistingTargetPosition> managed,
            Map<String, ExistingTargetPosition> attributed
    ) {
        if (request.targetPositionSnapshotStatus() == TargetPositionSnapshotStatus.UNAVAILABLE) {
            return DecisionCode.BLOCKED_TARGET_POSITION_SNAPSHOT_UNAVAILABLE;
        }
        if (request.targetPositionSnapshotStatus() == TargetPositionSnapshotStatus.STALE) {
            return DecisionCode.BLOCKED_TARGET_POSITION_SNAPSHOT_STALE;
        }
        if (!sameQuantities(actual, managed)) {
            return DecisionCode.BLOCKED_EXISTING_EXPOSURE_CONFLICT;
        }
        for (Map.Entry<String, ExistingTargetPosition> entry : attributed.entrySet()) {
            ExistingTargetPosition accountManaged = managed.get(entry.getKey());
            if (accountManaged == null
                    || entry.getValue().quantity().compareTo(accountManaged.quantity()) > 0) {
                return DecisionCode.BLOCKED_EXISTING_EXPOSURE_CONFLICT;
            }
        }
        return null;
    }

    private boolean sameQuantities(Map<String, ExistingTargetPosition> left,
                                   Map<String, ExistingTargetPosition> right) {
        if (!left.keySet().equals(right.keySet())) {
            return false;
        }
        for (String key : left.keySet()) {
            if (left.get(key).quantity().compareTo(right.get(key).quantity()) != 0) {
                return false;
            }
        }
        return true;
    }

    private BigDecimal availableTargetMargin(TargetPortfolioRequest request) {
        BigDecimal capitalAfterReservations = request.targetAllocatedCapitalUsd()
                .subtract(request.reservedMarginUsd())
                .max(DecimalSupport.ZERO);
        BigDecimal exchangeTotalCapacity = request.availableMarginUsd()
                .add(request.usedMarginUsd())
                .max(DecimalSupport.ZERO);
        return DecimalSupport.min(capitalAfterReservations, exchangeTotalCapacity);
    }

    private BigDecimal rawTargetNotional(TargetPortfolioRequest request, SourcePosition position) {
        if (request.sourceAccountEquityUsd() == null
                || request.sourceAccountEquityUsd().compareTo(DecimalSupport.ZERO) <= 0) {
            return DecimalSupport.ZERO;
        }
        BigDecimal exposure = DecimalSupport.divideDown(position.marginUsedUsd().abs(),
                request.sourceAccountEquityUsd());
        BigDecimal margin = DecimalSupport.normalize(request.targetAllocatedCapitalUsd().multiply(exposure));
        return DecimalSupport.normalize(margin.multiply(request.targetLeverage()));
    }

    private Map<String, BinanceSymbolFilter> indexFilters(List<BinanceSymbolFilter> values) {
        Map<String, BinanceSymbolFilter> result = new HashMap<>();
        for (BinanceSymbolFilter filter : values) {
            BinanceSymbolFilter previous = result.putIfAbsent(filter.symbol(), filter);
            if (previous != null && !previous.equals(filter)) {
                throw new IllegalArgumentException("conflicting filters for symbol " + filter.symbol());
            }
        }
        return result;
    }

    private Map<String, ExistingTargetPosition> aggregateExisting(List<ExistingTargetPosition> values) {
        Map<String, ExistingTargetPosition> result = new LinkedHashMap<>();
        for (ExistingTargetPosition position : values) {
            result.merge(position.key(), position, (left, right) -> new ExistingTargetPosition(
                    left.symbol(),
                    left.side(),
                    left.quantity().add(right.quantity()),
                    right.markPrice().compareTo(DecimalSupport.ZERO) > 0 ? right.markPrice() : left.markPrice(),
                    left.marginUsd().add(right.marginUsd())
            ));
        }
        return result;
    }

    private TargetLegDecision omitted(SourcePosition position, DecisionCode code, String detail) {
        return new TargetLegDecision(
                position.sourceLegId(), position.sourceSymbol(), safeSymbol(position.targetSymbol()), position.side(),
                false, code, detail, DecimalSupport.ZERO, DecimalSupport.ZERO, DecimalSupport.ZERO,
                DecimalSupport.ZERO, DecimalSupport.ZERO, DecimalSupport.ZERO, DecimalSupport.ZERO,
                DecimalSupport.ZERO, DecimalSupport.ZERO, DeltaAction.NONE, DecimalSupport.ZERO,
                position.liquidityScore(), false
        );
    }

    private TargetLegDecision omitted(Candidate candidate,
                                      DecisionCode code,
                                      String detail,
                                      BigDecimal rawQuantity,
                                      BigDecimal roundedQuantity) {
        SourcePosition position = candidate.position();
        BigDecimal scaledNotional = DecimalSupport.normalize(rawQuantity.multiply(candidate.price()));
        return new TargetLegDecision(
                position.sourceLegId(), position.sourceSymbol(), position.targetSymbol(), position.side(),
                false, code, detail, candidate.exposure(), candidate.rawTargetNotional(), scaledNotional,
                DecimalSupport.ZERO, rawQuantity, roundedQuantity, DecimalSupport.ZERO,
                DecimalSupport.ZERO, DecimalSupport.ZERO, DeltaAction.NONE,
                scaledNotional.subtract(roundedQuantity.multiply(candidate.price())).max(DecimalSupport.ZERO),
                candidate.liquidity(), false
        );
    }

    private static BigDecimal positivePrice(SourcePosition position) {
        if (position.markPrice().compareTo(DecimalSupport.ZERO) > 0) {
            return position.markPrice();
        }
        return position.entryPrice();
    }

    private static String key(String symbol, SourceSide side) {
        return safeSymbol(symbol) + "|" + side;
    }

    private static String safeSymbol(String symbol) {
        return symbol == null ? "" : symbol.trim().toUpperCase();
    }

    private static SourceSide opposite(SourceSide side) {
        return side == SourceSide.LONG ? SourceSide.SHORT : SourceSide.LONG;
    }

    private record Candidate(
            SourcePosition position,
            BinanceSymbolFilter filter,
            BigDecimal price,
            BigDecimal exposure,
            BigDecimal rawTargetNotional,
            BigDecimal rawTargetMargin,
            BigDecimal targetLeverage,
            BigDecimal liquidity,
            BigDecimal roundingError
    ) { }

    private record CandidateResolution(Candidate candidate, TargetLegDecision omitted) {
        static CandidateResolution candidate(Candidate candidate) {
            return new CandidateResolution(candidate, null);
        }

        static CandidateResolution omitted(TargetLegDecision omitted) {
            return new CandidateResolution(null, omitted);
        }
    }
}

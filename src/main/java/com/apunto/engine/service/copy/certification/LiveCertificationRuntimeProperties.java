package com.apunto.engine.service.copy.certification;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "copy.certification.runtime")
public class LiveCertificationRuntimeProperties {

    private String exchange = "BINANCE";
    private String strategyVersion = "copy-strategy-v3";
    private String sizingPolicyVersion = "proportional-portfolio-v3";
    private String symbolMappingVersion = "binance-symbol-map-v3";
    private String feeModelVersion = "binance-fee-v3";
    private String fundingModelVersion = "binance-funding-v3";
    private String slippageModelVersion = "binance-slippage-v3";
    private String liquidityModelVersion = "order-book-liquidity-v3";

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
    public String getStrategyVersion() { return strategyVersion; }
    public void setStrategyVersion(String strategyVersion) { this.strategyVersion = strategyVersion; }
    public String getSizingPolicyVersion() { return sizingPolicyVersion; }
    public void setSizingPolicyVersion(String sizingPolicyVersion) { this.sizingPolicyVersion = sizingPolicyVersion; }
    public String getSymbolMappingVersion() { return symbolMappingVersion; }
    public void setSymbolMappingVersion(String symbolMappingVersion) { this.symbolMappingVersion = symbolMappingVersion; }
    public String getFeeModelVersion() { return feeModelVersion; }
    public void setFeeModelVersion(String feeModelVersion) { this.feeModelVersion = feeModelVersion; }
    public String getFundingModelVersion() { return fundingModelVersion; }
    public void setFundingModelVersion(String fundingModelVersion) { this.fundingModelVersion = fundingModelVersion; }
    public String getSlippageModelVersion() { return slippageModelVersion; }
    public void setSlippageModelVersion(String slippageModelVersion) { this.slippageModelVersion = slippageModelVersion; }
    public String getLiquidityModelVersion() { return liquidityModelVersion; }
    public void setLiquidityModelVersion(String liquidityModelVersion) { this.liquidityModelVersion = liquidityModelVersion; }
}

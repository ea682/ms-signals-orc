package com.apunto.engine.service.copy.symbol;

public interface CopySymbolResolver {

    CopySymbolResolution resolve(String sourceSymbol, String capitalAsset);
}

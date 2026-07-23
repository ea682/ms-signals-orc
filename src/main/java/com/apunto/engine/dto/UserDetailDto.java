package com.apunto.engine.dto;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.EnumMap;
import java.util.Map;

@Data
@NoArgsConstructor
public class UserDetailDto {
    private UserEntity user;
    private DetailUserEntity detail;
    private UserApiKeyEntity userApiKey;
    private Map<ExecutionAccountPurpose, UserApiKeyEntity> executionAccounts =
            new EnumMap<>(ExecutionAccountPurpose.class);

    public UserDetailDto(UserEntity user, DetailUserEntity detail, UserApiKeyEntity userApiKey) {
        this.user = user;
        this.detail = detail;
        this.userApiKey = userApiKey;
        if (userApiKey != null && userApiKey.getAccountPurpose() != null) {
            executionAccounts.put(userApiKey.getAccountPurpose(), userApiKey);
        }
    }

    public void setExecutionAccounts(Map<ExecutionAccountPurpose, UserApiKeyEntity> accounts) {
        this.executionAccounts = new EnumMap<>(ExecutionAccountPurpose.class);
        if (accounts != null) this.executionAccounts.putAll(accounts);
    }

    public UserApiKeyEntity executionAccount(ExecutionAccountPurpose purpose) {
        return purpose == null || executionAccounts == null ? null : executionAccounts.get(purpose);
    }
}

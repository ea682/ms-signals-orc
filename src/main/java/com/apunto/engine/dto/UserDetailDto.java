package com.apunto.engine.dto;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDetailDto {
    private UserEntity user;
    private DetailUserEntity detail;
    private UserApiKeyEntity userApiKey;
}

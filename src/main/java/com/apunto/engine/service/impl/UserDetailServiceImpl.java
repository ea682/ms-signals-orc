package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserRepository;
import com.apunto.engine.service.UserDetailService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@AllArgsConstructor
public class UserDetailServiceImpl implements UserDetailService {

    private final UserRepository userRepository;
    private final DetailUserRepository detailUserRepository;
    private final UserApiKeyRepository userApiKeyRepository;

    @Override
    public List<UserDetailDto> findAllActive() {
        final List<UserDetailDto> usersDetailDtos = new ArrayList<>();
        final List<UserEntity> users = userRepository.findAll();

        for (UserEntity user : users) {
            if (user == null || user.getId() == null) {
                log.debug("event=user_detail.skip reason=user_missing");
                continue;
            }

            final DetailUserEntity detailUserEntity =
                    detailUserRepository.findByUser_Id_AndUserActive(user.getId(), Boolean.TRUE);
            if (detailUserEntity == null || !detailUserEntity.isUserActive()) {
                log.debug("event=user_detail.skip reason=detail_inactive_or_missing userId={}", user.getId());
                continue;
            }

            if (!detailUserEntity.isApiKeyBinar()) {
                log.debug("event=user_detail.skip reason=binance_api_key_inactive userId={}", user.getId());
                continue;
            }

            final UserApiKeyEntity userApiKeyEntity = userApiKeyRepository.findByUser_Id(user.getId());
            if (userApiKeyEntity == null) {
                log.warn("event=user_detail.skip reason=api_key_missing userId={}", user.getId());
                continue;
            }

            final UserDetailDto userDto = new UserDetailDto();
            userDto.setUser(user);
            userDto.setUserApiKey(userApiKeyEntity);
            userDto.setDetail(detailUserEntity);

            usersDetailDtos.add(userDto);
        }
        return usersDetailDtos;
    }
}

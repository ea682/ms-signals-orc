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
    public List<UserDetailDto> findAll() {
        List<UserDetailDto> usersDetailDtos = new ArrayList<>();
        List<UserEntity>  users = userRepository.findAll();

        for( UserEntity user : users ) {

            DetailUserEntity detailUserEntity = detailUserRepository.findByUser_Id(user.getId());
            UserApiKeyEntity userApiKeyEntity = userApiKeyRepository.findByUser_Id(user.getId());

            if(detailUserEntity.isUserActive() && detailUserEntity.isApiKeyBinar()){
                UserDetailDto userDto = new UserDetailDto();
                userDto.setUser(user);
                userDto.setUserApiKey(userApiKeyEntity);
                userDto.setDetail(detailUserEntity);

                usersDetailDtos.add(userDto);
            }
        }
        return usersDetailDtos;
    }
}

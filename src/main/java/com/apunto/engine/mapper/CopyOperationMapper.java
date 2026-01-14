package com.apunto.engine.mapper;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface CopyOperationMapper {

    CopyOperationEntity toEntity(CopyOperationDto dto);

    CopyOperationDto toDto(CopyOperationEntity entity);
}


package com.apunto.engine.mapper;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface CopyOperationMapper {

    CopyOperationEntity toEntity(CopyOperationDto dto);

    CopyOperationDto toDto(CopyOperationEntity entity);
}

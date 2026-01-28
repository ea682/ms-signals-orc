package com.apunto.engine.entity.converter;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * Convierte el enum Status a texto en minúsculas para persistirlo en Postgres.
 * Ej: Status.ACTIVE <-> "active".
 */
@Converter(autoApply = false)
public class UserCopyAllocationStatusConverter
        implements AttributeConverter<UserCopyAllocationEntity.Status, String> {

    @Override
    public String convertToDatabaseColumn(UserCopyAllocationEntity.Status attribute) {
        if (attribute == null) {
            return null;
        }
        return attribute.name().toLowerCase();
    }

    @Override
    public UserCopyAllocationEntity.Status convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        return UserCopyAllocationEntity.Status.valueOf(dbData.trim().toUpperCase());
    }
}

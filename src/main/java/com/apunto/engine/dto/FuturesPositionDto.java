package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.PositionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FuturesPositionDto {

    private UUID idFuturesPosition;
    private PositionStatus isActive;
}


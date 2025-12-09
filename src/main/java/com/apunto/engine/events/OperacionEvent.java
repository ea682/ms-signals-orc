package com.apunto.engine.events;

import com.apunto.engine.dto.OperacionDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OperacionEvent {

    public enum Tipo {
        ABIERTA,
        CERRADA
    }

    private Tipo tipo;
    private OperacionDto operacion;
}
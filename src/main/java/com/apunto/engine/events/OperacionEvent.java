package com.apunto.engine.events;

import com.apunto.engine.dto.OperacionDto;
import com.fasterxml.jackson.annotation.JsonAlias;
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

    /**
     * Compatibilidad: el producer nuevo (futures_position) publica el payload bajo la propiedad
     * "position" en lugar de "operacion". Con @JsonAlias aceptamos ambos sin romper
     * el resto del motor.
     */
    @JsonAlias("position")
    private OperacionDto operacion;
}
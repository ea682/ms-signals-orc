package com.apunto.copytarget.runner;

import com.apunto.copytarget.CopyPolicyV1SizingStepResult;
import com.apunto.copytarget.CopyPolicyV1SizingStepRunner;
import com.fasterxml.jackson.databind.ObjectMapper;

final class CopyPolicyV1NdjsonRunner {

    private final ObjectMapper mapper;
    private final CopyPolicyV1SizingStepRunner runner;

    CopyPolicyV1NdjsonRunner() {
        this(new ObjectMapper().findAndRegisterModules(), new CopyPolicyV1SizingStepRunner());
    }

    CopyPolicyV1NdjsonRunner(ObjectMapper mapper, CopyPolicyV1SizingStepRunner runner) {
        this.mapper = mapper;
        this.runner = runner;
    }

    String processLine(String line) {
        String requestId = null;
        try {
            CopyPolicyV1Protocol.StepRequest request = mapper.readValue(
                    line, CopyPolicyV1Protocol.StepRequest.class);
            requestId = request.requestId();
            CopyPolicyV1SizingStepResult result = runner.run(request.toCore());
            return mapper.writeValueAsString(new Response(requestId, true, result, null, null));
        } catch (Exception failure) {
            return error(requestId, failure);
        }
    }

    private String error(String requestId, Exception failure) {
        try {
            return mapper.writeValueAsString(new Response(
                    requestId, false, null, "COPY_POLICY_V1_INPUT_REJECTED",
                    failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage()));
        } catch (Exception serializationFailure) {
            return "{\"ok\":false,\"errorCode\":\"COPY_POLICY_V1_RESPONSE_FAILED\"}";
        }
    }

    record Response(
            String requestId,
            boolean ok,
            CopyPolicyV1SizingStepResult result,
            String errorCode,
            String errorMessage
    ) {
    }
}

package com.apunto.copytarget.runner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public final class CopyPolicyV1RunnerMain {

    private CopyPolicyV1RunnerMain() {
    }

    public static void main(String[] args) throws Exception {
        CopyPolicyV1NdjsonRunner runner = new CopyPolicyV1NdjsonRunner();
        try (BufferedReader input = new BufferedReader(new InputStreamReader(
                System.in, StandardCharsets.UTF_8));
             BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                     System.out, StandardCharsets.UTF_8))) {
            String line;
            while ((line = input.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                output.write(runner.processLine(line));
                output.newLine();
                output.flush();
            }
        }
    }
}

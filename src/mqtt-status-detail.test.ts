import { describe, it, expect } from "vitest";
import { describeMqttError, DEFAULT_MQTT_URL } from "./mqtt-status-detail.js";

// Errors as mqtt.js 5 emits them (probed on 2026-11-25).
const err = (code: unknown, message: string) => Object.assign(new Error(message), { code });

describe("describeMqttError (spec 216)", () => {
  it("points at extra_hosts when host.docker.internal does not resolve", () => {
    const text = describeMqttError(err("ENOTFOUND", "getaddrinfo ENOTFOUND host.docker.internal"), DEFAULT_MQTT_URL);
    expect(text).toMatch(/^Cannot resolve host\.docker\.internal/);
    expect(text).toContain("extra_hosts");
  });

  it("names any other unresolved host", () => {
    expect(describeMqttError(err("ENOTFOUND", "x"), "mqtt://brokr.lan")).toBe(
      "Cannot resolve brokr.lan. Check the broker address.",
    );
  });

  it("explains localhost inside Docker on a refused connection", () => {
    const text = describeMqttError(err("ECONNREFUSED", "connect ECONNREFUSED 127.0.0.1:1883"), "mqtt://127.0.0.1:1883");
    expect(text).toMatch(/^Connection refused on 127\.0\.0\.1:1883/);
    expect(text).toContain(DEFAULT_MQTT_URL);
  });

  it("points at the Mosquitto listener on any other refused connection", () => {
    const text = describeMqttError(err("ECONNREFUSED", "x"), "mqtt://host.docker.internal");
    expect(text).toMatch(/^Connection refused on host\.docker\.internal:1883/);
    expect(text).toContain("listener 1883 0.0.0.0");
  });

  it("reads a connack timeout as no answer", () => {
    expect(describeMqttError(err(undefined, "connack timeout"), "mqtts://10.0.0.9")).toBe(
      "No answer from 10.0.0.9:8883. Check the broker address and the firewall.",
    );
  });

  it("reads a broker refusal as rejected credentials", () => {
    expect(describeMqttError(err(5, "Connection refused: Not authorized"), DEFAULT_MQTT_URL)).toMatch(
      /^Credentials rejected/,
    );
  });

  it("keeps the raw message for anything else", () => {
    expect(describeMqttError(err("EPIPE", "write EPIPE"), DEFAULT_MQTT_URL)).toBe("MQTT error: write EPIPE");
    expect(describeMqttError("boom", "not a url")).toBe("MQTT error: boom");
  });
});

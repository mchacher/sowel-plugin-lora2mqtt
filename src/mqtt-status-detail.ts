/**
 * Spec 216 (sowel): turns an MQTT connection error into one sentence the user
 * can act on. Sowel shows it under the integration status while the plugin is
 * not connected (`getStatusDetail()`).
 *
 * Same file in every Sowel MQTT plugin: change them together.
 */

const DEFAULT_PORTS: Record<string, string> = { "mqtt:": "1883", "mqtts:": "8883", "ws:": "80", "wss:": "443" };
const LOOPBACK = new Set(["localhost", "127.0.0.1", "::1", "[::1]", "0.0.0.0"]);

/** Default broker URL proposed by the settings form: a broker on the Docker host. */
export const DEFAULT_MQTT_URL = "mqtt://host.docker.internal:1883";

function broker(url: string): { host: string; where: string } {
  try {
    const u = new URL(url);
    const host = u.hostname || url;
    const port = u.port || DEFAULT_PORTS[u.protocol] || "";
    return { host, where: port ? `${host}:${port}` : host };
  } catch {
    return { host: url, where: url };
  }
}

export function describeMqttError(err: unknown, url: string): string {
  const e = (err ?? {}) as { code?: unknown; message?: unknown };
  const code = e.code;
  const message = typeof e.message === "string" ? e.message : String(err);
  const { host, where } = broker(url);

  if (code === "ENOTFOUND" || code === "EAI_AGAIN") {
    if (host === "host.docker.internal") {
      return 'Cannot resolve host.docker.internal. Add extra_hosts: ["host.docker.internal:host-gateway"] to the sowel service in docker-compose.yml and recreate the container, or use the host\'s LAN IP.';
    }
    return `Cannot resolve ${host}. Check the broker address.`;
  }
  if (code === "ECONNREFUSED") {
    if (LOOPBACK.has(host)) {
      return `Connection refused on ${where}. Inside Docker, ${host} is the Sowel container itself: use ${DEFAULT_MQTT_URL} or the host's LAN IP.`;
    }
    return `Connection refused on ${where}. Check that the broker runs and listens beyond localhost (Mosquitto: listener 1883 0.0.0.0).`;
  }
  if (
    code === "ETIMEDOUT" ||
    code === "EHOSTUNREACH" ||
    code === "ENETUNREACH" ||
    /connack timeout/i.test(message)
  ) {
    return `No answer from ${where}. Check the broker address and the firewall.`;
  }
  if (code === 4 || code === 5 || code === 134 || code === 135 || /not authori[sz]ed|bad user ?name or password/i.test(message)) {
    return "Credentials rejected by the broker. Check the MQTT username and password.";
  }
  return `MQTT error: ${message}`;
}

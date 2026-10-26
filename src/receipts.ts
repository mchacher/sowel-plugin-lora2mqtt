/**
 * Order delivery receipts (Sowel spec 188, R6).
 *
 * The lora2mqtt bridge echoes the `#id` of an order on `<base>/<node>/receipt`
 * (bridge spec command-receipt, C2/C9/C10/C18). This module correlates those
 * outcomes with the orders the plugin sent and turns them into
 * `device.order.receipt` events. It holds no MQTT client, so it can be tested
 * on its own.
 */

export type OrderReceiptStatus =
  | "pending" | "received" | "delivered" | "refused" | "not_received" | "unconfirmed";

export interface OrderReceiptEvent {
  type: "device.order.receipt";
  integrationId: string;
  deviceId: string;
  orderKey: string;
  status: OrderReceiptStatus;
  late?: boolean;
}

/** Bridge `result` → Sowel status. */
const RESULT_TO_STATUS: Record<string, OrderReceiptStatus> = {
  received: "received",
  applied: "delivered",
  refused: "refused",
  not_received: "not_received",
  timeout: "unconfirmed",
};

/** No terminal outcome from the bridge within this delay → we say `unconfirmed` ourselves. */
export const RECEIPT_FALLBACK_MS = 20_000;
/** A terminal entry is kept this long so a late receipt still maps to its order. */
export const RECEIPT_RETENTION_MS = 120_000;

interface PendingOrder {
  deviceId: string;
  orderKey: string;
  /** Last status emitted for this order. */
  status: OrderReceiptStatus;
  timer: ReturnType<typeof setTimeout>;
}

interface ReceiptLogger {
  debug(obj: Record<string, unknown>, msg: string): void;
}

export class ReceiptTracker {
  private pending = new Map<string, PendingOrder>();

  constructor(
    private readonly integrationId: string,
    private readonly emit: (event: OrderReceiptEvent) => void,
    private readonly logger: ReceiptLogger,
  ) {}

  /** Register an order just published with `#id` = id, and emit `pending`. */
  track(id: string, deviceId: string, orderKey: string): void {
    const entry: PendingOrder = {
      deviceId, orderKey, status: "pending",
      timer: setTimeout(() => this.fallback(id), RECEIPT_FALLBACK_MS),
    };
    this.pending.set(id, entry);
    this.send(entry, "pending");
  }

  /** Handle one payload from `<base>/<node>/receipt`. */
  handleReceipt(raw: unknown): void {
    if (typeof raw !== "object" || raw === null) return;
    const msg = raw as Record<string, unknown>;
    const id = typeof msg.id === "string" ? msg.id : null;
    const entry = id ? this.pending.get(id) : undefined;
    if (!id || !entry) {
      this.logger.debug({ id: msg.id ?? null, result: msg.result }, "Receipt for an unknown order, ignored");
      return;
    }
    const status = typeof msg.result === "string" ? RESULT_TO_STATUS[msg.result] : undefined;
    if (!status) {
      this.logger.debug({ id, result: msg.result }, "Receipt with an unknown result, ignored");
      return;
    }

    const wasTerminal = isTerminal(entry.status);
    // The bridge's own `timeout` after our fallback already said `unconfirmed`: nothing new.
    if (status === entry.status && wasTerminal) return;

    // Late: the bridge says so, or we already gave a terminal verdict (our 20 s fallback).
    const late = msg.late === true || wasTerminal;
    entry.status = status;
    this.send(entry, status, late);

    if (isTerminal(status)) {
      clearTimeout(entry.timer);
      entry.timer = setTimeout(() => this.pending.delete(id), RECEIPT_RETENTION_MS);
    }
  }

  /** Forget everything and cancel all timers (plugin stop). */
  clear(): void {
    for (const entry of this.pending.values()) clearTimeout(entry.timer);
    this.pending.clear();
  }

  get size(): number { return this.pending.size; }

  private fallback(id: string): void {
    const entry = this.pending.get(id);
    if (!entry) return;
    entry.status = "unconfirmed";
    this.send(entry, "unconfirmed");
    // Keep it a while so a late bridge message for this id still maps.
    entry.timer = setTimeout(() => this.pending.delete(id), RECEIPT_RETENTION_MS);
  }

  private send(entry: PendingOrder, status: OrderReceiptStatus, late = false): void {
    const event: OrderReceiptEvent = {
      type: "device.order.receipt",
      integrationId: this.integrationId,
      deviceId: entry.deviceId,
      orderKey: entry.orderKey,
      status,
    };
    if (late) event.late = true;
    this.emit(event);
  }
}

function isTerminal(status: OrderReceiptStatus): boolean {
  return status !== "pending" && status !== "received";
}

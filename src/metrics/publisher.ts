/**
 * Network Metric Publisher
 *
 * Publishes one Sparkplug B UDT message per network interface on
 * network.data.{interfaceName}. The message carries the full interface
 * state as a structured value with a NetworkInterfaceTemplate definition,
 * so tentacle-mqtt can publish it as a proper Sparkplug B Template Instance.
 *
 * Only publishes when the interface's aggregated value changes
 * (change detection via previous serialized value map).
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import type {
  NetworkInterface,
  NetworkInterfaceConfig,
  PlcDataMessage,
} from "@tentacle/nats-schema";
import { NetworkInterfaceTemplate } from "@tentacle/nats-schema";
import { log } from "../utils/logger.ts";

// Access logger dynamically so it picks up the wrapped logger after enableNatsLogging
const logger = {
  get info() { return log.service.info.bind(log.service); },
  get debug() { return log.service.debug.bind(log.service); },
  get warn() { return log.service.warn.bind(log.service); },
};

/** Prefixes of virtual/container interfaces to exclude from metric publishing */
const VIRTUAL_INTERFACE_PREFIXES = [
  "veth",    // Docker/container veth pairs
  "br-",     // Docker bridge networks
  "docker",  // Docker default bridge
  "tap",     // TAP devices (VMs)
  "vnet",    // libvirt virtual network
  "virbr",   // libvirt virtual bridges
  "dummy",   // dummy interfaces
  "incusbr", // Incus/LXD bridges
];

function isVirtualInterface(name: string): boolean {
  return VIRTUAL_INTERFACE_PREFIXES.some((prefix) => name.startsWith(prefix));
}

/** Previous serialized values for change detection (interfaceName → JSON string) */
const previousValues = new Map<string, string>();

const encoder = new TextEncoder();

/**
 * Build the flat UDT value object for a network interface.
 * Array fields are JSON-stringified so all members are primitives.
 */
function buildInterfaceValue(
  iface: NetworkInterface,
  cfg: NetworkInterfaceConfig | undefined,
): Record<string, unknown> {
  return {
    operstate: iface.operstate,
    carrier: iface.carrier,
    speed: iface.speed ?? 0,
    duplex: iface.duplex ?? "unknown",
    mac: iface.mac,
    mtu: iface.mtu,
    addresses: JSON.stringify(
      iface.addresses.map((a) => `${a.address}/${a.prefixlen}`),
    ),
    rx_bytes: iface.statistics.rxBytes,
    tx_bytes: iface.statistics.txBytes,
    rx_packets: iface.statistics.rxPackets,
    tx_packets: iface.statistics.txPackets,
    rx_errors: iface.statistics.rxErrors,
    tx_errors: iface.statistics.txErrors,
    rx_dropped: iface.statistics.rxDropped,
    tx_dropped: iface.statistics.txDropped,
    config_dhcp4: cfg?.dhcp4 ?? false,
    config_addresses: JSON.stringify(cfg?.addresses ?? []),
    config_gateway4: cfg?.gateway4 ?? "",
    config_nameservers: JSON.stringify(cfg?.nameservers ?? []),
    config_mtu: cfg?.mtu ?? 0,
  };
}

/**
 * Publish changed network interfaces as UDT PlcDataMessages to NATS.
 *
 * Each interface is published as a single message with datatype "udt"
 * and a NetworkInterfaceTemplate definition, enabling tentacle-mqtt to
 * create a proper Sparkplug B Template Instance metric.
 *
 * @returns Number of interfaces published
 */
export function publishNetworkMetrics(
  nc: NatsConnection,
  interfaces: NetworkInterface[],
  configs: NetworkInterfaceConfig[],
): number {
  const configMap = new Map<string, NetworkInterfaceConfig>();
  for (const cfg of configs) {
    configMap.set(cfg.interfaceName, cfg);
  }

  let publishCount = 0;

  for (const iface of interfaces) {
    if (iface.name === "lo" || isVirtualInterface(iface.name)) continue;

    const cfg = configMap.get(iface.name);
    const value = buildInterfaceValue(iface, cfg);
    const serialized = JSON.stringify(value);

    if (previousValues.get(iface.name) === serialized) continue;
    previousValues.set(iface.name, serialized);

    const msg: PlcDataMessage = {
      moduleId: "network",
      deviceId: "network",
      variableId: iface.name,
      value,
      datatype: "udt",
      udtTemplate: NetworkInterfaceTemplate,
      description: `Network interface ${iface.name}`,
      timestamp: Date.now(),
    };

    nc.publish(
      `network.data.${iface.name}`,
      encoder.encode(JSON.stringify(msg)),
    );
    publishCount++;
  }

  if (publishCount > 0) {
    logger.debug(`Published ${publishCount} changed network interface(s)`);
  }

  return publishCount;
}

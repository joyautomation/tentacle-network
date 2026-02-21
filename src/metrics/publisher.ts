/**
 * Network Metric Publisher
 *
 * Flattens NetworkInterface[] + NetworkInterfaceConfig[] into individual
 * PlcDataMessage publishes on network.data.{variableId}. Only publishes
 * when a metric value changes (change detection via previous value map).
 *
 * This allows tentacle-mqtt to pick up network metrics automatically
 * via its existing *.data.> subscription and bridge them to Sparkplug B.
 */

import type { NatsConnection } from "@nats-io/transport-deno";
import type {
  NetworkInterface,
  NetworkInterfaceConfig,
  PlcDataMessage,
} from "@tentacle/nats-schema";
import { log } from "../utils/logger.ts";

// Access logger dynamically so it picks up the wrapped logger after enableNatsLogging
const logger = {
  get info() { return log.service.info.bind(log.service); },
  get debug() { return log.service.debug.bind(log.service); },
  get warn() { return log.service.warn.bind(log.service); },
};

type MetricValue = number | boolean | string;

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

interface MetricDef {
  variableId: string;
  value: MetricValue;
  datatype: "number" | "boolean" | "string";
  description: string;
}

/** Previous metric values for change detection (variableId → serialized value) */
const previousValues = new Map<string, string>();

const encoder = new TextEncoder();

/**
 * Serialize a value for comparison. Uses JSON.stringify for consistent
 * comparison of all types including strings with special characters.
 */
function serialize(value: MetricValue): string {
  return JSON.stringify(value);
}

/**
 * Extract live status metrics from a NetworkInterface.
 */
function extractLiveMetrics(iface: NetworkInterface): MetricDef[] {
  const name = iface.name;
  const metrics: MetricDef[] = [
    { variableId: `${name}/operstate`, value: iface.operstate, datatype: "string", description: `${name} - Operational State` },
    { variableId: `${name}/carrier`, value: iface.carrier, datatype: "boolean", description: `${name} - Carrier` },
    { variableId: `${name}/speed`, value: iface.speed ?? 0, datatype: "number", description: `${name} - Speed (Mbps)` },
    { variableId: `${name}/duplex`, value: iface.duplex ?? "unknown", datatype: "string", description: `${name} - Duplex` },
    { variableId: `${name}/mac`, value: iface.mac, datatype: "string", description: `${name} - MAC Address` },
    { variableId: `${name}/mtu`, value: iface.mtu, datatype: "number", description: `${name} - MTU` },
  ];

  // Flatten addresses to JSON array of CIDR strings
  const addrList = iface.addresses.map(
    (a) => `${a.address}/${a.prefixlen}`,
  );
  metrics.push({
    variableId: `${name}/addresses`,
    value: JSON.stringify(addrList),
    datatype: "string",
    description: `${name} - Addresses`,
  });

  return metrics;
}

/**
 * Extract config metrics from a NetworkInterfaceConfig.
 */
function extractConfigMetrics(config: NetworkInterfaceConfig): MetricDef[] {
  const name = config.interfaceName;
  return [
    { variableId: `${name}/config/dhcp4`, value: config.dhcp4 ?? false, datatype: "boolean", description: `${name} - DHCP4 Enabled` },
    { variableId: `${name}/config/addresses`, value: JSON.stringify(config.addresses ?? []), datatype: "string", description: `${name} - Configured Addresses` },
    { variableId: `${name}/config/gateway4`, value: config.gateway4 ?? "", datatype: "string", description: `${name} - Gateway` },
    { variableId: `${name}/config/nameservers`, value: JSON.stringify(config.nameservers ?? []), datatype: "string", description: `${name} - Nameservers` },
    { variableId: `${name}/config/mtu`, value: config.mtu ?? 0, datatype: "number", description: `${name} - Configured MTU` },
  ];
}

/**
 * Publish changed network metrics as individual PlcDataMessage to NATS.
 *
 * Called after each interface state poll. Compares current values to
 * previous and only publishes metrics that have changed.
 *
 * @returns Number of metrics published
 */
export function publishNetworkMetrics(
  nc: NatsConnection,
  interfaces: NetworkInterface[],
  configs: NetworkInterfaceConfig[],
): number {
  const allMetrics: MetricDef[] = [];

  // Build config lookup by interface name
  const configMap = new Map<string, NetworkInterfaceConfig>();
  for (const cfg of configs) {
    configMap.set(cfg.interfaceName, cfg);
  }

  for (const iface of interfaces) {
    // Skip loopback and virtual/container interfaces
    if (iface.name === "lo" || isVirtualInterface(iface.name)) continue;

    // Live status metrics
    allMetrics.push(...extractLiveMetrics(iface));

    // Config metrics (if config exists for this interface)
    const cfg = configMap.get(iface.name);
    if (cfg) {
      allMetrics.push(...extractConfigMetrics(cfg));
    }
  }

  // Also publish config metrics for interfaces that don't appear in live state
  // (shouldn't happen normally, but be defensive)
  for (const cfg of configs) {
    if (!interfaces.some((i) => i.name === cfg.interfaceName)) {
      allMetrics.push(...extractConfigMetrics(cfg));
    }
  }

  let publishCount = 0;

  for (const metric of allMetrics) {
    const serialized = serialize(metric.value);
    const prev = previousValues.get(metric.variableId);

    // Only publish if value changed
    if (prev === serialized) continue;

    previousValues.set(metric.variableId, serialized);

    const msg: PlcDataMessage = {
      moduleId: "network",
      deviceId: "network",
      variableId: metric.variableId,
      value: metric.value,
      timestamp: Date.now(),
      datatype: metric.datatype,
      description: metric.description,
    };

    const subject = `network.data.${metric.variableId}`;
    nc.publish(subject, encoder.encode(JSON.stringify(msg)));
    publishCount++;
  }

  if (publishCount > 0) {
    logger.debug(`Published ${publishCount} changed network metric(s)`);
  }

  return publishCount;
}

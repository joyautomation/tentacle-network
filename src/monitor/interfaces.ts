/**
 * Network Interface Monitor
 *
 * Reads actual live state from the Linux kernel via sysfs and ip.
 * Never caches, never reports desired state — always ground truth.
 */

import type {
  NetworkInterface,
  NetworkInterfaceStats,
  NetworkAddress,
} from "@tentacle/nats-schema";
import { log } from "../utils/logger.ts";

const SYSFS_NET = "/sys/class/net";

// ═══════════════════════════════════════════════════════════════════════════════
// sysfs helpers
// ═══════════════════════════════════════════════════════════════════════════════

async function readSysfs(
  ifaceName: string,
  attr: string,
): Promise<string | null> {
  try {
    const content = await Deno.readTextFile(`${SYSFS_NET}/${ifaceName}/${attr}`);
    return content.trim();
  } catch {
    return null;
  }
}

async function readSysfsInt(
  ifaceName: string,
  attr: string,
): Promise<number | null> {
  const val = await readSysfs(ifaceName, attr);
  if (val === null) return null;
  const num = parseInt(val, 10);
  return isNaN(num) ? null : num;
}

async function readStatistics(ifaceName: string): Promise<NetworkInterfaceStats> {
  const statsDir = `${SYSFS_NET}/${ifaceName}/statistics`;
  const readStat = async (name: string): Promise<number> => {
    try {
      const val = await Deno.readTextFile(`${statsDir}/${name}`);
      return parseInt(val.trim(), 10) || 0;
    } catch {
      return 0;
    }
  };

  const [
    rxBytes,
    txBytes,
    rxPackets,
    txPackets,
    rxErrors,
    txErrors,
    rxDropped,
    txDropped,
  ] = await Promise.all([
    readStat("rx_bytes"),
    readStat("tx_bytes"),
    readStat("rx_packets"),
    readStat("tx_packets"),
    readStat("rx_errors"),
    readStat("tx_errors"),
    readStat("rx_dropped"),
    readStat("tx_dropped"),
  ]);

  return {
    rxBytes,
    txBytes,
    rxPackets,
    txPackets,
    rxErrors,
    txErrors,
    rxDropped,
    txDropped,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// ip -j addr show
// ═══════════════════════════════════════════════════════════════════════════════

/** Entry from `ip -j addr show` */
type IpAddrEntry = {
  ifname: string;
  flags: string[];
  addr_info: Array<{
    family: string;
    local: string;
    prefixlen: number;
    scope: string;
    broadcast?: string;
  }>;
};

async function getIpAddrJson(): Promise<IpAddrEntry[]> {
  try {
    const cmd = new Deno.Command("ip", {
      args: ["-j", "addr", "show"],
      stdout: "piped",
      stderr: "piped",
    });
    const result = await cmd.output();
    if (!result.success) {
      const stderr = new TextDecoder().decode(result.stderr);
      log.monitor.warn(`ip -j addr show failed: ${stderr}`);
      return [];
    }
    const stdout = new TextDecoder().decode(result.stdout);
    return JSON.parse(stdout) as IpAddrEntry[];
  } catch (err) {
    log.monitor.warn(`Failed to run ip -j addr show: ${err}`);
    return [];
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main monitoring function
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Read actual live state of all network interfaces from the kernel.
 * Every call reads fresh — no caching.
 */
export async function getNetworkInterfaces(): Promise<NetworkInterface[]> {
  // Phase 1: Enumerate interfaces from sysfs
  const ifaceNames: string[] = [];
  try {
    for await (const entry of Deno.readDir(SYSFS_NET)) {
      if (entry.isDirectory || entry.isSymlink) {
        ifaceNames.push(entry.name);
      }
    }
  } catch (err) {
    log.monitor.error(`Failed to enumerate ${SYSFS_NET}: ${err}`);
    return [];
  }

  ifaceNames.sort();

  // Phase 2: Get IP addresses and flags from ip -j addr show (one call for all)
  const ipData = await getIpAddrJson();
  const ipByName = new Map<string, IpAddrEntry>();
  for (const entry of ipData) {
    ipByName.set(entry.ifname, entry);
  }

  // Phase 3: Build interface objects
  const interfaces: NetworkInterface[] = [];

  for (const name of ifaceNames) {
    const [operstate, carrierStr, speed, duplex, mac, mtu, iftype, statistics] =
      await Promise.all([
        readSysfs(name, "operstate"),
        readSysfs(name, "carrier"),
        readSysfsInt(name, "speed"),
        readSysfs(name, "duplex"),
        readSysfs(name, "address"),
        readSysfsInt(name, "mtu"),
        readSysfsInt(name, "type"),
        readStatistics(name),
      ]);

    // Get addresses and flags from ip data
    const ipEntry = ipByName.get(name);
    const addresses: NetworkAddress[] = (ipEntry?.addr_info ?? [])
      .filter((a) => a.family === "inet" || a.family === "inet6")
      .map((a) => ({
        family: a.family as "inet" | "inet6",
        address: a.local,
        prefixlen: a.prefixlen,
        scope: a.scope,
        broadcast: a.broadcast,
      }));

    interfaces.push({
      name,
      operstate: operstate ?? "unknown",
      carrier: carrierStr === "1",
      speed: speed,
      duplex: duplex,
      mac: mac ?? "00:00:00:00:00:00",
      mtu: mtu ?? 0,
      type: iftype ?? 0,
      flags: ipEntry?.flags ?? [],
      addresses,
      statistics,
    });
  }

  return interfaces;
}

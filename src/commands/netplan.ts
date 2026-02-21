/**
 * Netplan Configuration Manager
 *
 * Manages persistent network configuration via netplan.
 * On get-config: reads and merges ALL /etc/netplan/*.yaml files.
 * On apply-config: writes to 60-tentacle.yaml and backs up other yaml files
 * to .yaml.bak so tentacle becomes the sole config source.
 * All changes are persistent (survive reboot).
 */

import { stringify as yamlStringify, parse as yamlParse } from "@std/yaml";
import type { NetworkInterfaceConfig } from "@tentacle/nats-schema";
import { log } from "../utils/logger.ts";

const NETPLAN_DIR = "/etc/netplan";
const TENTACLE_NETPLAN_PATH = `${NETPLAN_DIR}/60-tentacle.yaml`;

// Input validation patterns
const IFACE_NAME_PATTERN = /^[a-zA-Z0-9._-]+$/;

// ═══════════════════════════════════════════════════════════════════════════════
// Validation
// ═══════════════════════════════════════════════════════════════════════════════

function validateInterfaceConfig(config: NetworkInterfaceConfig): string | null {
  if (!IFACE_NAME_PATTERN.test(config.interfaceName)) {
    return `Invalid interface name: ${config.interfaceName}`;
  }

  if (config.addresses) {
    for (const addr of config.addresses) {
      if (!addr.includes("/")) {
        return `Address must include prefix length (e.g. 192.168.1.100/24): ${addr}`;
      }
    }
  }

  if (config.mtu !== undefined && (config.mtu < 68 || config.mtu > 65535)) {
    return `MTU must be between 68 and 65535: ${config.mtu}`;
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// YAML generation
// ═══════════════════════════════════════════════════════════════════════════════

/** Netplan YAML document structure */
type NetplanDocument = {
  network: {
    version: number;
    ethernets: Record<string, NetplanEthernetConfig>;
  };
};

type NetplanEthernetConfig = {
  dhcp4?: boolean;
  addresses?: string[];
  routes?: Array<{ to: string; via: string }>;
  nameservers?: { addresses: string[] };
  mtu?: number;
};

/**
 * Build a netplan YAML document from our interface configs.
 */
export function buildNetplanYaml(interfaces: NetworkInterfaceConfig[]): string {
  const ethernets: Record<string, NetplanEthernetConfig> = {};

  for (const iface of interfaces) {
    const entry: NetplanEthernetConfig = {};

    if (iface.dhcp4 !== undefined) {
      entry.dhcp4 = iface.dhcp4;
    }

    if (iface.addresses && iface.addresses.length > 0) {
      entry.addresses = iface.addresses;
    }

    if (iface.gateway4) {
      entry.routes = [{ to: "default", via: iface.gateway4 }];
    }

    if (iface.nameservers && iface.nameservers.length > 0) {
      entry.nameservers = { addresses: iface.nameservers };
    }

    if (iface.mtu !== undefined) {
      entry.mtu = iface.mtu;
    }

    ethernets[iface.interfaceName] = entry;
  }

  const doc: NetplanDocument = {
    network: {
      version: 2,
      ethernets,
    },
  };

  return yamlStringify(doc);
}

/**
 * Parse a netplan YAML document back into our interface config types.
 */
function parseNetplanYaml(yaml: string): NetworkInterfaceConfig[] {
  const doc = yamlParse(yaml) as NetplanDocument | null;
  if (!doc?.network?.ethernets) return [];

  const configs: NetworkInterfaceConfig[] = [];

  for (const [ifaceName, entry] of Object.entries(doc.network.ethernets)) {
    const config: NetworkInterfaceConfig = {
      interfaceName: ifaceName,
    };

    if (entry.dhcp4 !== undefined) config.dhcp4 = entry.dhcp4;
    if (entry.addresses) config.addresses = entry.addresses;
    if (entry.mtu !== undefined) config.mtu = entry.mtu;

    // Extract gateway from routes
    if (entry.routes) {
      const defaultRoute = entry.routes.find((r) => r.to === "default");
      if (defaultRoute) config.gateway4 = defaultRoute.via;
    }

    // Extract nameservers
    if (entry.nameservers?.addresses) {
      config.nameservers = entry.nameservers.addresses;
    }

    configs.push(config);
  }

  return configs;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Read / Write
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Read and merge ALL netplan yaml files in /etc/netplan/.
 * Files are read in sorted order (matching netplan's own behavior).
 * Later files override earlier ones for the same interface.
 */
export async function readTentacleConfig(): Promise<NetworkInterfaceConfig[]> {
  const merged = new Map<string, NetworkInterfaceConfig>();

  try {
    const files: string[] = [];
    for await (const entry of Deno.readDir(NETPLAN_DIR)) {
      if (entry.isFile && entry.name.endsWith(".yaml")) {
        files.push(entry.name);
      }
    }
    files.sort(); // netplan processes in lexicographic order

    for (const file of files) {
      try {
        const content = await Deno.readTextFile(`${NETPLAN_DIR}/${file}`);
        const configs = parseNetplanYaml(content);
        for (const cfg of configs) {
          // Later files override earlier ones (same as netplan merge behavior)
          merged.set(cfg.interfaceName, cfg);
        }
      } catch (err) {
        log.cmd.warn(`Failed to parse ${file}: ${err}`);
      }
    }
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) {
      return [];
    }
    log.cmd.warn(`Failed to read ${NETPLAN_DIR}: ${err}`);
    return [];
  }

  return Array.from(merged.values());
}

/**
 * Back up all non-tentacle netplan yaml files by renaming to .yaml.bak.
 * This makes tentacle the sole config source.
 */
async function backupOtherNetplanFiles(): Promise<void> {
  try {
    for await (const entry of Deno.readDir(NETPLAN_DIR)) {
      if (
        entry.isFile &&
        entry.name.endsWith(".yaml") &&
        entry.name !== "60-tentacle.yaml"
      ) {
        const src = `${NETPLAN_DIR}/${entry.name}`;
        const dst = `${src}.bak`;
        log.cmd.info(`Backing up ${entry.name} -> ${entry.name}.bak`);
        await Deno.rename(src, dst);
      }
    }
  } catch (err) {
    log.cmd.warn(`Failed to back up netplan files: ${err}`);
  }
}

/**
 * Write interface configs to 60-tentacle.yaml, back up other yaml files,
 * and run netplan apply.
 * Returns null on success, error string on failure.
 */
export async function applyTentacleConfig(
  interfaces: NetworkInterfaceConfig[],
): Promise<string | null> {
  // Validate all configs first
  for (const config of interfaces) {
    const err = validateInterfaceConfig(config);
    if (err) return err;
  }

  // Build YAML
  const yaml = buildNetplanYaml(interfaces);
  log.cmd.info(`Writing netplan config to ${TENTACLE_NETPLAN_PATH}`);
  log.cmd.debug(`Config:\n${yaml}`);

  // Write tentacle config file
  try {
    await Deno.writeTextFile(TENTACLE_NETPLAN_PATH, yaml);
  } catch (err) {
    const msg = `Failed to write ${TENTACLE_NETPLAN_PATH}: ${err}`;
    log.cmd.error(msg);
    return msg;
  }

  // Back up other netplan files so tentacle is the sole source
  await backupOtherNetplanFiles();

  // Apply
  try {
    const cmd = new Deno.Command("netplan", {
      args: ["apply"],
      stdout: "piped",
      stderr: "piped",
    });
    const result = await cmd.output();

    if (!result.success) {
      const stderr = new TextDecoder().decode(result.stderr).trim();
      const msg = `netplan apply failed: ${stderr}`;
      log.cmd.error(msg);
      return msg;
    }

    log.cmd.info("netplan apply succeeded");
    return null;
  } catch (err) {
    const msg = `Failed to run netplan apply: ${err}`;
    log.cmd.error(msg);
    return msg;
  }
}

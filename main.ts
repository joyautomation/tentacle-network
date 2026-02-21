/**
 * tentacle-network Service
 *
 * Monitors Linux network interfaces (actual state from kernel via sysfs)
 * and provides persistent configuration management via netplan + NATS.
 *
 * Environment variables:
 *   NATS_SERVERS - NATS server URL(s), comma-separated (default: localhost:4222)
 *
 * Run with:
 *   deno run --allow-net --allow-read --allow-write=/etc/netplan --allow-env --allow-run=ip,netplan main.ts
 */

import { connect, type NatsConnection } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { enableNatsLogging, log as logMap } from "./src/utils/logger.ts";
import { getNetworkInterfaces } from "./src/monitor/interfaces.ts";
import {
  readTentacleConfig,
  applyTentacleConfig,
} from "./src/commands/netplan.ts";
import { publishNetworkMetrics } from "./src/metrics/publisher.ts";
import type {
  ServiceHeartbeat,
  NetworkStateMessage,
  NetworkCommandRequest,
  NetworkCommandResponse,
  NetworkInterfaceConfig,
} from "@tentacle/nats-schema";
import { isNetworkCommandRequest } from "@tentacle/nats-schema";

// Access logMap.service dynamically so it picks up the wrapped logger after enableNatsLogging
const log = {
  get info() { return logMap.service.info.bind(logMap.service); },
  get warn() { return logMap.service.warn.bind(logMap.service); },
  get error() { return logMap.service.error.bind(logMap.service); },
  get debug() { return logMap.service.debug.bind(logMap.service); },
};

// ═══════════════════════════════════════════════════════════════════════════════
// NATS Connection with retry
// ═══════════════════════════════════════════════════════════════════════════════

async function connectToNats(servers: string): Promise<NatsConnection> {
  const serverList = servers.split(",").map((s) => s.trim());

  while (true) {
    try {
      log.info(`Connecting to NATS at ${servers}...`);
      const nc = await connect({ servers: serverList });
      log.info("Connected to NATS");
      return nc;
    } catch (err) {
      log.warn(`Failed to connect to NATS: ${err}. Retrying in 5 seconds...`);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

async function main(): Promise<void> {
  log.info("═══════════════════════════════════════════════════════════════");
  log.info("            tentacle-network Service");
  log.info("═══════════════════════════════════════════════════════════════");

  const natsServers = Deno.env.get("NATS_SERVERS") || "localhost:4222";
  log.info(`Module ID: network`);
  log.info(`NATS Servers: ${natsServers}`);

  // Connect to NATS (retries forever)
  const nc = await connectToNats(natsServers);

  // Enable NATS log streaming for all loggers
  enableNatsLogging(nc, "network", "network");

  // Handle NATS connection close
  (async () => {
    const err = await nc.closed();
    if (err) {
      log.error(`NATS connection closed with error: ${err}`);
    }
  })();

  const encoder = new TextEncoder();

  // ═══════════════════════════════════════════════════════════════════════════
  // Periodic interface state publishing (actual state from kernel)
  // ═══════════════════════════════════════════════════════════════════════════

  let lastInterfaceCount = 0;
  let lastConfigs: NetworkInterfaceConfig[] = [];

  async function publishNetworkState(): Promise<void> {
    try {
      const interfaces = await getNetworkInterfaces();
      lastInterfaceCount = interfaces.length;
      const snapshot: NetworkStateMessage = {
        moduleId: "network",
        timestamp: Date.now(),
        interfaces,
      };
      nc.publish(
        "network.interfaces",
        encoder.encode(JSON.stringify(snapshot)),
      );

      // Also read config and publish individual metrics for Sparkplug B
      try {
        lastConfigs = await readTentacleConfig();
      } catch {
        // Config read may fail if no netplan files exist yet
      }
      publishNetworkMetrics(nc, interfaces, lastConfigs);
    } catch (err) {
      log.warn(`Failed to collect/publish network state: ${err}`);
    }
  }

  // Initial publish
  await publishNetworkState();
  log.info(`Published initial state: ${lastInterfaceCount} interfaces`);

  // Periodic publish every 10s
  const publishInterval = setInterval(publishNetworkState, 10_000);

  // ═══════════════════════════════════════════════════════════════════════════
  // On-demand state request (request/reply — always fresh from kernel)
  // ═══════════════════════════════════════════════════════════════════════════

  const stateSub = nc.subscribe("network.state");
  const stateAbort = new AbortController();

  (async () => {
    try {
      for await (const msg of stateSub) {
        if (stateAbort.signal.aborted) break;
        try {
          // Always read fresh — never serve stale data
          const interfaces = await getNetworkInterfaces();
          const snapshot: NetworkStateMessage = {
            moduleId: "network",
            timestamp: Date.now(),
            interfaces,
          };
          msg.respond(encoder.encode(JSON.stringify(snapshot)));
        } catch (err) {
          msg.respond(
            encoder.encode(JSON.stringify({ error: String(err) })),
          );
        }
      }
    } catch (error) {
      if (!stateAbort.signal.aborted) {
        log.error("Error in state request handler:", error);
      }
    }
  })();

  log.info("Listening for state requests on: network.state");

  // ═══════════════════════════════════════════════════════════════════════════
  // Configuration commands (request/reply — persistent via netplan)
  // ═══════════════════════════════════════════════════════════════════════════

  const commandSub = nc.subscribe("network.command");
  const commandAbort = new AbortController();

  (async () => {
    try {
      for await (const msg of commandSub) {
        if (commandAbort.signal.aborted) break;
        try {
          const request = JSON.parse(
            new TextDecoder().decode(msg.data),
          ) as NetworkCommandRequest;

          if (!isNetworkCommandRequest(request)) {
            const resp: NetworkCommandResponse = {
              requestId:
                (request as Record<string, unknown>).requestId as string ??
                  "unknown",
              success: false,
              error: "Invalid command request",
              timestamp: Date.now(),
            };
            msg.respond(encoder.encode(JSON.stringify(resp)));
            continue;
          }

          log.info(`Network command: ${request.action}`);

          if (request.action === "get-config") {
            const config = await readTentacleConfig();
            const resp: NetworkCommandResponse = {
              requestId: request.requestId,
              success: true,
              config,
              timestamp: Date.now(),
            };
            msg.respond(encoder.encode(JSON.stringify(resp)));
          } else if (request.action === "apply-config") {
            if (!request.interfaces || request.interfaces.length === 0) {
              const resp: NetworkCommandResponse = {
                requestId: request.requestId,
                success: false,
                error: "No interfaces provided for apply-config",
                timestamp: Date.now(),
              };
              msg.respond(encoder.encode(JSON.stringify(resp)));
              continue;
            }

            const error = await applyTentacleConfig(request.interfaces);
            const resp: NetworkCommandResponse = {
              requestId: request.requestId,
              success: error === null,
              error: error ?? undefined,
              timestamp: Date.now(),
            };
            msg.respond(encoder.encode(JSON.stringify(resp)));

            // After successful apply, re-publish fresh actual state
            if (error === null) {
              // Brief delay for networking to settle
              await new Promise((r) => setTimeout(r, 2000));
              await publishNetworkState();
              log.info("Re-published network state after config apply");
            }
          } else if (request.action === "add-address") {
            if (!request.interfaceName || !request.address) {
              const resp: NetworkCommandResponse = {
                requestId: request.requestId,
                success: false,
                error: "interfaceName and address are required for add-address",
                timestamp: Date.now(),
              };
              msg.respond(encoder.encode(JSON.stringify(resp)));
              continue;
            }

            log.info(`Adding address ${request.address} to ${request.interfaceName}`);
            const cmd = new Deno.Command("ip", {
              args: ["addr", "add", request.address, "dev", request.interfaceName],
              stdout: "piped",
              stderr: "piped",
            });
            const { code, stderr } = await cmd.output();
            const errText = new TextDecoder().decode(stderr);

            // Exit code 2 with "File exists" means address already present — treat as success
            const alreadyExists = code === 2 && errText.includes("File exists");
            const addSuccess = code === 0 || alreadyExists;

            const resp: NetworkCommandResponse = {
              requestId: request.requestId,
              success: addSuccess,
              error: addSuccess ? undefined : `ip addr add failed (exit ${code}): ${errText}`,
              timestamp: Date.now(),
            };
            msg.respond(encoder.encode(JSON.stringify(resp)));

            if (addSuccess) {
              await publishNetworkState();
            }
          } else if (request.action === "remove-address") {
            if (!request.interfaceName || !request.address) {
              const resp: NetworkCommandResponse = {
                requestId: request.requestId,
                success: false,
                error: "interfaceName and address are required for remove-address",
                timestamp: Date.now(),
              };
              msg.respond(encoder.encode(JSON.stringify(resp)));
              continue;
            }

            log.info(`Removing address ${request.address} from ${request.interfaceName}`);
            const cmd = new Deno.Command("ip", {
              args: ["addr", "del", request.address, "dev", request.interfaceName],
              stdout: "piped",
              stderr: "piped",
            });
            const { code, stderr } = await cmd.output();
            const errText = new TextDecoder().decode(stderr);

            // "Cannot assign" means address not found — treat as success
            const notFound = code !== 0 && (errText.includes("Cannot assign") || errText.includes("No such process"));
            const delSuccess = code === 0 || notFound;

            const resp: NetworkCommandResponse = {
              requestId: request.requestId,
              success: delSuccess,
              error: delSuccess ? undefined : `ip addr del failed (exit ${code}): ${errText}`,
              timestamp: Date.now(),
            };
            msg.respond(encoder.encode(JSON.stringify(resp)));

            if (delSuccess) {
              await publishNetworkState();
            }
          }
        } catch (err) {
          const resp: NetworkCommandResponse = {
            requestId: "unknown",
            success: false,
            error: String(err),
            timestamp: Date.now(),
          };
          msg.respond(encoder.encode(JSON.stringify(resp)));
        }
      }
    } catch (error) {
      if (!commandAbort.signal.aborted) {
        log.error("Error in command handler:", error);
      }
    }
  })();

  log.info("Listening for commands on: network.command");

  // ═══════════════════════════════════════════════════════════════════════════
  // Individual field commands (from Sparkplug B DCMD routing)
  // ═══════════════════════════════════════════════════════════════════════════

  const fieldCommandSub = nc.subscribe("network.command.>");
  const fieldCommandAbort = new AbortController();

  (async () => {
    try {
      for await (const msg of fieldCommandSub) {
        if (fieldCommandAbort.signal.aborted) break;
        try {
          // Extract variableId from subject: network.command.{variableId}
          const variableId = msg.subject.slice("network.command.".length);

          // Only handle config writes (ignore writes to live status metrics)
          if (!variableId.includes("/config/")) {
            log.debug(`Ignoring field command for non-config metric: ${variableId}`);
            continue;
          }

          const rawValue = new TextDecoder().decode(msg.data);
          log.info(`Field command: ${variableId} = ${rawValue}`);

          // Parse: "eth0/config/dhcp4" → interfaceName="eth0", field="dhcp4"
          const configIdx = variableId.indexOf("/config/");
          const interfaceName = variableId.slice(0, configIdx);
          const field = variableId.slice(configIdx + "/config/".length);

          // Read current config
          const configs = await readTentacleConfig();

          // Find or create config entry for this interface
          let ifaceConfig = configs.find(
            (c) => c.interfaceName === interfaceName,
          );
          if (!ifaceConfig) {
            ifaceConfig = { interfaceName };
            configs.push(ifaceConfig);
          }

          // Update the specific field
          switch (field) {
            case "dhcp4":
              ifaceConfig.dhcp4 = rawValue === "true" || rawValue === "1";
              break;
            case "addresses":
              try {
                ifaceConfig.addresses = JSON.parse(rawValue) as string[];
              } catch {
                log.warn(`Invalid JSON for addresses: ${rawValue}`);
                continue;
              }
              break;
            case "nameservers":
              try {
                ifaceConfig.nameservers = JSON.parse(rawValue) as string[];
              } catch {
                log.warn(`Invalid JSON for nameservers: ${rawValue}`);
                continue;
              }
              break;
            case "gateway4":
              ifaceConfig.gateway4 = rawValue || undefined;
              break;
            case "mtu": {
              const mtu = parseInt(rawValue, 10);
              ifaceConfig.mtu = isNaN(mtu) ? undefined : mtu;
              break;
            }
            default:
              log.warn(`Unknown config field: ${field}`);
              continue;
          }

          // Apply the updated config via netplan
          const error = await applyTentacleConfig(configs);
          if (error) {
            log.error(`Failed to apply config for ${variableId}: ${error}`);
          } else {
            log.info(`Applied config change: ${variableId} = ${rawValue}`);
            // Brief delay for networking to settle, then re-publish metrics
            await new Promise((r) => setTimeout(r, 2000));
            await publishNetworkState();
          }
        } catch (err) {
          log.error(`Error handling field command: ${err}`);
        }
      }
    } catch (error) {
      if (!fieldCommandAbort.signal.aborted) {
        log.error("Error in field command handler:", error);
      }
    }
  })();

  log.info("Listening for field commands on: network.command.>");

  // ═══════════════════════════════════════════════════════════════════════════
  // Heartbeat publishing for service discovery
  // ═══════════════════════════════════════════════════════════════════════════

  const js = jetstream(nc);
  const kvm = new Kvm(js);
  const heartbeatsKv = await kvm.create("service_heartbeats", {
    history: 1,
    ttl: 60 * 1000,
  });

  const heartbeatKey = "network";
  const startedAt = Date.now();

  const publishHeartbeat = async () => {
    const heartbeat: ServiceHeartbeat = {
      serviceType: "network",
      moduleId: "network",
      lastSeen: Date.now(),
      startedAt,
      metadata: {
        interfaceCount: lastInterfaceCount,
      },
    };
    try {
      await heartbeatsKv.put(
        heartbeatKey,
        encoder.encode(JSON.stringify(heartbeat)),
      );
    } catch (err) {
      log.warn(`Failed to publish heartbeat: ${err}`);
    }
  };

  await publishHeartbeat();
  log.info("Service heartbeat started (moduleId: network)");

  const heartbeatInterval = setInterval(publishHeartbeat, 10000);

  log.info("");
  log.info("Service running. Press Ctrl+C to stop.");
  log.info("");

  // ═══════════════════════════════════════════════════════════════════════════
  // Graceful shutdown
  // ═══════════════════════════════════════════════════════════════════════════

  const shutdown = async (signal: string) => {
    log.info(`Received ${signal}, shutting down...`);

    clearInterval(publishInterval);
    clearInterval(heartbeatInterval);

    stateAbort.abort();
    commandAbort.abort();
    fieldCommandAbort.abort();

    try {
      await heartbeatsKv.delete(heartbeatKey);
      log.info("Removed service heartbeat");
    } catch {
      // Ignore - may already be expired
    }

    await nc.drain();
    log.info("Shutdown complete");
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", () => shutdown("SIGINT"));
  Deno.addSignalListener("SIGTERM", () => shutdown("SIGTERM"));

  // Listen for NATS shutdown command
  const shutdownSub = nc.subscribe("network.shutdown");
  (async () => {
    for await (const _msg of shutdownSub) {
      log.info("Received shutdown command via NATS");
      await shutdown("NATS shutdown");
      break;
    }
  })();
}

// Run
if (import.meta.main) {
  main().catch((err) => {
    log.error(`Fatal error: ${err}`);
    Deno.exit(1);
  });
}

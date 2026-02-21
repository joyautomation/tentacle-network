import { createLogger, LogLevel, type Log } from "@joyautomation/coral";
import type { NatsConnection } from "@nats-io/transport-deno";
import type { ServiceLogEntry } from "@tentacle/nats-schema";

export { LogLevel, type Log };

let currentLevel = LogLevel.info;

export const log: Record<string, Log> = {
  service: createLogger("network", currentLevel),
  monitor: createLogger("network:monitor", currentLevel),
  cmd: createLogger("network:cmd", currentLevel),
};

export function setLogLevel(level: LogLevel) {
  currentLevel = level;
}

/**
 * Wrap a coral logger to also publish log entries to NATS.
 */
function wrapLogger(
  coralLog: Log,
  publishFn: (level: string, loggerName: string, msg: string) => void,
  loggerName: string,
): Log {
  const formatArgs = (args: unknown[]): string =>
    args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");

  return {
    info: (msg: string, ...args: unknown[]) => {
      coralLog.info(msg, ...args);
      publishFn("info", loggerName, args.length > 0 ? `${msg} ${formatArgs(args)}` : msg);
    },
    warn: (msg: string, ...args: unknown[]) => {
      coralLog.warn(msg, ...args);
      publishFn("warn", loggerName, args.length > 0 ? `${msg} ${formatArgs(args)}` : msg);
    },
    error: (msg: string, ...args: unknown[]) => {
      coralLog.error(msg, ...args);
      publishFn("error", loggerName, args.length > 0 ? `${msg} ${formatArgs(args)}` : msg);
    },
    debug: (msg: string, ...args: unknown[]) => {
      coralLog.debug(msg, ...args);
      publishFn("debug", loggerName, args.length > 0 ? `${msg} ${formatArgs(args)}` : msg);
    },
  } as Log;
}

/**
 * Upgrade all exported loggers to also publish to NATS.
 * Call once after NATS connects. Since `log` is a shared object,
 * all modules that imported it will see the wrapped loggers.
 */
export function enableNatsLogging(
  nc: NatsConnection,
  serviceType: string,
  moduleId: string,
): void {
  const subject = `service.logs.${serviceType}.${moduleId}`;
  const encoder = new TextEncoder();

  const publishFn = (level: string, loggerName: string, message: string) => {
    try {
      const entry: ServiceLogEntry = {
        timestamp: Date.now(),
        level: level as ServiceLogEntry["level"],
        message,
        serviceType,
        moduleId,
        logger: loggerName,
      };
      nc.publish(subject, encoder.encode(JSON.stringify(entry)));
    } catch {
      // Never let log publishing break the service
    }
  };

  // Wrap each logger in-place
  for (const key of Object.keys(log)) {
    log[key] = wrapLogger(log[key], publishFn, `network:${key}`);
  }
}

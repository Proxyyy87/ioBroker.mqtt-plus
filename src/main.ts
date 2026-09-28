"use strict";

/*
 * MQTT-Bridge-Manager
 * Feature: Force-Sync Interval (5 min) bypassing cache to heal split-brain/async states
 */

import * as utils from "@iobroker/adapter-core";
import axios from "axios";
import * as http from "node:http";
import * as https from "node:https";
import * as crypto from "node:crypto";
import * as net from "node:net";
import * as os from "node:os";

// Einmalig aus package.json gelesen, statt an mehreren Stellen (User-Agent, info.version)
// manuell zu pflegen und bei jedem Versionssprung zu vergessen.
const ADAPTER_VERSION: string = require("../package.json").version;

interface MappingEntry {
    id: string;
    mqttName: string;
    dir: "in" | "out" | "both";
    type: "auto" | "round" | "boolToNum" | "numToBool";
    unit?: string;
    // "confirmed" (Standard) verlangt ack=true (echtes Gerät bestätigt).
    // "any" akzeptiert auch ack=false - für 0_userdata.0.*/alias.0.* ohne Gerät dahinter.
    ackFilter?: "confirmed" | "any";
    decimals?: number;
    // "single" (Standard): ein Topic für Status UND Befehl - bisheriges Verhalten.
    // "dual": Status wird auf <topic> gemeldet, Befehle kommen auf <topic>/set herein
    // (übliche MQTT-Konvention: Schreibzugriffe mit /set-Suffix, Statusmeldungen ohne Suffix).
    topicMode?: "single" | "dual";
    // Nur bei "dual": Suffix des Befehls-Topics. Standard "/set".
    commandSuffix?: string;
    // Aktualitäts-Grenze in Minuten für diesen Eintrag: leer = globaler Wert, 0 = keine Prüfung.
    // Ist das letzte Update der Quelle älter, wird bei Start/Cycle/Force-Sync nicht gespiegelt.
    staleAfterMin?: number | string | null;
    // "standard" (Standard): nur echte Änderungen, Original-Zeitstempel, inaktive Quellen ruhen.
    // "refresh": zusätzlich jede neue Meldung der Quelle mit gleichem Wert weiterreichen (ts
    //            wandert mit) - für Sensoren, die korrekt messen, deren Wert sich aber kaum ändert.
    // "force": Verhalten vor 1.6.0 - Force-Sync schreibt immer mit ts = jetzt, ohne Aktualitäts-
    //          prüfung. Das Ziel wirkt dadurch immer frisch, auch wenn das Gerät nicht mehr meldet.
    syncMode?: "standard" | "refresh" | "force";
    // Zur Laufzeit berechnet: Status-Topic (Ziel der Richtung IOB -> MQTT).
    fullTargetPath?: string;
    // Zur Laufzeit berechnet: Befehls-Topic (Quelle der Richtung MQTT -> IOB).
    // Bei "single" identisch mit fullTargetPath.
    commandPath?: string;
}

interface SyncOptions {
    // Cache ignorieren (Force-Sync).
    force?: boolean;
    decimals?: number;
    // Nur bei ackFilter:"any" nötig (siehe pendingWrites-Kommentar) - überall sonst hat die
    // Ack-Prüfung ein eigenes Echo bereits ausgeschlossen; der Schutz würde dort nur eine echte,
    // spätere Bestätigung fälschlich verschlucken.
    useEchoGuard?: boolean;
    // Start/Cycle/Force-Sync: Quelle muss aktiv sein (q == 0, ts nicht älter als staleLimitMs).
    // Bei Events entfällt die Prüfung - das Ereignis selbst belegt die Aktivität.
    requireAlive?: boolean;
    staleLimitMs?: number;
    // Aktueller Zielwert für den Vergleich: ist er schon korrekt, wird nicht geschrieben.
    targetState?: ioBroker.State | null;
    // ts/lc/q der Quelle übernehmen (Zustands-Spiegelung). false bei Befehlen (dual /set).
    preserveTimestamp?: boolean;
    // syncMode "refresh": gleicher Wert mit neuerem Quell-ts wird trotzdem geschrieben.
    passRefresh?: boolean;
    // Befehl über das Befehls-Topic (dual /set): jeder Befehl ist neu und wird nie am
    // Wert-Cache verworfen. Der Cache kennt nur den letzten BEFEHL, nicht den Gerätezustand -
    // wurde das Gerät dazwischen anderweitig geschaltet (Taster, App), wäre ein gleicher
    // Befehl sonst wirkungslos.
    isCommand?: boolean;
}

type SyncPhase = "event" | "start" | "cycle" | "force";

declare global {
    namespace ioBroker {
        interface AdapterConfig {
            targetBasePath: string;
            updateIntervalSec: number;
            logTransfers: boolean;
            mappings: MappingEntry[];
            syncUrl: string;
            syncIntervalMin: number;
            // ioBroker-Konvention: Port und Bind-Adresse heißen "port" und "bind".
            port: number;
            bind: string;
            // Bis 1.6.2 verwendete Namen - werden in migrateLegacyConfig() einmalig übernommen.
            serverPort?: number;
            bindHost?: string;
            dashboardUser: string;
            dashboardPassword: string;
            dashboardTlsCert: string;
            dashboardTlsKey: string;
            syncCaCert: string;
            forceSyncIntervalMin: number;
            staleAfterMin: number;
            remoteSyncSkipStale: boolean;
        }
    }
}

class MqttPlus extends utils.Adapter {
    // Obergrenze für POST-Bodies (Backup-Upload / Sync-Template) gegen Memory-Exhaustion
    private static readonly MAX_BODY_BYTES = 5 * 1024 * 1024; // 5 MB
    private static readonly MAX_AUTH_ATTEMPTS = 10;
    private static readonly AUTH_LOCKOUT_MS = 5 * 60 * 1000; // 5 Minuten
    private static readonly REMOTE_SYNC_CHUNK_SIZE = 200;
    // Sicherheitsnetz für pendingWrites: falls auf einen eigenen Schreibvorgang nie ein
    // (echtes oder Echo-)Ereignis folgt, verfällt der Merker statt für immer liegenzubleiben.
    private static readonly PENDING_WRITE_TTL_MS = 10_000;
    // Port-Konflikt beim Start: so oft neu versuchen, bevor der Adapter aufgibt.
    private static readonly LISTEN_ATTEMPTS = 6;
    private static readonly LISTEN_RETRY_MS = 5_000;
    // Wartezeit auf den Neustart durch js-controller nach der Konfigurationsmigration.
    private static readonly MIGRATION_RESTART_FALLBACK_MS = 30_000;
    // Standard-Aktualitätsgrenze, falls in der Instanz (z.B. nach Update von <1.6.0) nichts gesetzt ist.
    private static readonly DEFAULT_STALE_AFTER_MIN = 1440; // 24 h

    // Map statt Plain Object: verhindert Prototype Pollution über Datenpunkt-/Topic-Namen wie "__proto__"
    private lastSyncValues: Map<string, any> = new Map();
    // Quell-ts des zuletzt geschriebenen Werts je Verbindung - Basis für syncMode "refresh".
    private lastSyncTs: Map<string, number> = new Map();
    private updateInterval: ioBroker.Interval | undefined = undefined;
    private forceSyncInterval: ioBroker.Interval | undefined = undefined;
    private syncInterval: ioBroker.Interval | undefined = undefined;
    private httpServer: http.Server | https.Server | undefined = undefined;
    private currentWatchdogStatus: string = "Init";
    private unloaded = false;
    private syncRunning = false;
    private activeSockets: Set<net.Socket> = new Set();

    // Performance Lookup Maps
    private sourceToMappings: Map<string, MappingEntry[]> = new Map();
    private targetToMappings: Map<string, MappingEntry[]> = new Map();

    // common.type des jeweiligen Zielobjekts (aus ensureAdapterObject) - Basis für Auto-Konvertierung
    private targetTypeCache: Map<string, ioBroker.CommonType> = new Map();
    // common.type des jeweiligen Quellobjekts, einmalig beim Setup ermittelt (für /api/json)
    private sourceTypeCache: Map<string, string> = new Map();

    // Konsumierbarer Echo-Schutz: merkt sich pro Ziel-ID den zuletzt selbst geschriebenen Wert,
    // bis entweder das passende Echo eintrifft (verbraucht) oder ein abweichender Wert eintrifft
    // (verworfen - echte Änderung). Nur wirksam, wenn syncValue() mit useEchoGuard=true läuft -
    // sonst hätte die Ack-Prüfung in onStateChange ein echtes eigenes Echo bereits ausgeschlossen,
    // und der Schutz würde nur eine spätere, echte Bestätigung fälschlich verschlucken.
    private pendingWrites: Map<string, { value: any; ts: number }> = new Map();

    // Quellen, die aktuell als inaktiv (veraltet/schlechte Qualität) gelten - nur für das
    // Logging beim Zustandswechsel, damit nicht jeder Cycle dieselbe Meldung wiederholt.
    private staleSources: Set<string> = new Set();

    // Brute-Force-Schutz für das Dashboard-Login, pro Client-IP
    private failedAuthAttempts: Map<string, { count: number; lockedUntil: number }> = new Map();

    private lastWatchdogStatus = "";
    private lastWatchdogWriteTs = 0;

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({
            ...options,
            name: "mqtt-plus",
        });
        this.on("ready", this.onReady.bind(this));
        this.on("stateChange", this.onStateChange.bind(this));
        this.on("unload", this.onUnload.bind(this));
        this.on("message", this.onMessage.bind(this));
    }

    // this.delay() statt eines eigenen setTimeout: wird beim Unload vom Adapter automatisch
    // aufgeräumt (Voraussetzung für Compact Mode).
    private sleep(ms: number): Promise<void> {
        return this.delay(ms);
    }

    private convertMqttPathToIobrokerId(mqttPath: string): string {
        if (!mqttPath) return "unknown";
        let cleaned = mqttPath.replace(/^[\/\.]+|[\/\.]+$/g, "");
        cleaned = cleaned.replace(/\//g, ".");
        cleaned = cleaned.replace(/\s+/g, "_");
        // Zusätzlich zu / und . verbietet ioBroker weitere Zeichen in IDs (*?,;'"`<>[])
        cleaned = cleaned.replace(this.FORBIDDEN_CHARS, "_");
        return cleaned || "unknown";
    }

    // Ermittelt das Befehls-Topic eines Mappings.
    // "single" (Standard): Befehl und Status teilen sich ein Topic - wie bisher.
    // "dual": Befehle laufen über <topic>/set, der Status bleibt auf <topic> - dadurch sind
    // Schreib- und Leserichtung physisch getrennt und können sich nicht gegenseitig auslösen.
    private resolveCommandPath(entry: MappingEntry, statePath: string): string {
        if (entry.topicMode !== "dual") return statePath;

        const raw = (entry.commandSuffix || "").trim() || "/set";
        const suffix = this.convertMqttPathToIobrokerId(raw);
        if (!suffix || suffix === "unknown") {
            this.log.warn(`[Setup] Invalid command suffix "${raw}" for "${entry.id}" - using "/set".`);
            return `${statePath}.set`;
        }
        return `${statePath}.${suffix}`;
    }

    // Liest den konfigurierten Ziel-Präfix mit Absicherung gegen leere/fehlerhafte Config.
    private getValidatedBasePath(): string {
        let base = (this.config.targetBasePath || "").trim();
        if (!base) {
            this.log.warn("[Config] No MQTT target path configured - using default 'mqtt.0.'");
            base = "mqtt.0.";
        }
        return base.endsWith(".") ? base : base + ".";
    }

    // Übernimmt die bis 1.6.2 verwendeten Konfigurationsnamen serverPort/bindHost einmalig in
    // port/bind und entfernt die alten Schlüssel. Das Schreiben des Instanzobjekts lässt den
    // js-controller die Instanz mit der neuen Konfiguration neu starten - deshalb gibt die
    // Funktion true zurück, und onReady bricht diesen Start ab.
    private async migrateLegacyConfig(): Promise<boolean> {
        const legacy = this.config as ioBroker.AdapterConfig & Record<string, unknown>;
        if (legacy.serverPort === undefined && legacy.bindHost === undefined) return false;

        const objId = `system.adapter.${this.namespace}`;
        const obj = await this.getForeignObjectAsync(objId);
        if (!obj || !obj.native) return false;

        const native = obj.native as Record<string, unknown>;
        if (native.serverPort !== undefined) native.port = native.serverPort;
        if (native.bindHost !== undefined) native.bind = native.bindHost;
        delete native.serverPort;
        delete native.bindHost;

        this.log.info(`[Setup] Configuration migrated: serverPort/bindHost -> port=${native.port}, bind=${native.bind}. Instance restarts.`);
        await this.setForeignObjectAsync(objId, obj);
        // Normalerweise startet js-controller die Instanz wegen der geänderten Konfiguration
        // selbst neu. Falls das ausbleibt, fordert der Adapter den Neustart selbst an - sonst
        // bliebe er ohne Webserver und Sync stehen. Adapter-Timer werden beim Unload aufgeräumt,
        // ein regulärer Neustart vorher macht diesen Timer also wirkungslos.
        this.setTimeout(() => {
            this.log.warn("[Setup] No restart after configuration migration - restarting instance.");
            this.restart();
        }, MqttPlus.MIGRATION_RESTART_FALLBACK_MS);
        return true;
    }

    private async onReady(): Promise<void> {
        if (await this.migrateLegacyConfig()) return;

        await this.initObjects();
        await this.loadAuthLockouts();
        await this.setStateAsync("info.version", ADAPTER_VERSION, true);
        this.log.info(`[Setup] MQTT bridge manager v${ADAPTER_VERSION} starting...`);
        const staleMin = this.parseMinutes(this.config.staleAfterMin) ?? MqttPlus.DEFAULT_STALE_AFTER_MIN;
        this.log.info(staleMin > 0
            ? `[Setup] Staleness check: sources without update for ${staleMin} min are not mirrored on start/cycle/force sync.`
            : "[Setup] Staleness check (age) disabled globally - only quality (q) is checked.");
        await this.setupBridge();

        const port = this.config.port || 8095;
        const hasTls = !!(this.config.dashboardTlsCert && this.config.dashboardTlsKey);
        if (!this.config.dashboardPassword) {
            this.log.warn("[Dashboard] No password set - the web server is reachable without access protection! Please set a password in the adapter settings (tab 'Web Dashboard').");
        } else if (!hasTls) {
            this.log.warn("[Dashboard] A password is set, but no TLS certificate is configured - credentials are transmitted unencrypted (HTTP). Add certificate/key in the tab 'Web Dashboard' for encryption.");
        }
        this.startWebServer(port);
        await this.updateDashboardUrlState(port);

        // 1. Normales Fallback-Intervall (Mit Cache), mindestens 5s gegen Busy-Loop bei Fehlkonfiguration
        const updateIntervalSec = Math.max(5, this.config.updateIntervalSec || 60);
        this.log.info(`Starting update interval (fallback): ${updateIntervalSec} seconds`);
        this.updateInterval = this.setInterval(() => {
            this.runCycleSync().catch(e => this.log.error(`Cycle sync error: ${e.message}`));
        }, updateIntervalSec * 1000);

        // 2. Force-Sync Intervall (ohne Cache zur System-Heilung), konfigurierbar/abschaltbar
        const forceSyncMin = this.config.forceSyncIntervalMin ?? 5;
        if (forceSyncMin > 0) {
            this.log.info(`Starting force sync interval: every ${forceSyncMin} minutes (heals out-of-sync states)`);
            this.forceSyncInterval = this.setInterval(() => {
                this.runForceSync().catch(e => this.log.error(`Force sync error: ${e.message}`));
            }, forceSyncMin * 60 * 1000);
        } else {
            this.log.info("Force sync interval disabled (configuration).");
        }

        // 3. Remote Sync
        if (this.config.syncUrl) {
            const syncIntervalMin = this.config.syncIntervalMin || 60;
            this.log.info(`Starting remote sync: every ${syncIntervalMin} minutes`);
            this.runRemoteSync(false).catch(e => this.log.error(`Remote sync error: ${e.message}`));
            this.syncInterval = this.setInterval(() => {
                this.runRemoteSync(false).catch(e => this.log.error(`Remote sync error: ${e.message}`));
            }, syncIntervalMin * 60 * 1000);
        }

        this.updateWatchdog("Running", true);
    }

    private async updateDashboardUrlState(port: number): Promise<void> {
        let ip = "127.0.0.1";
        try {
            const ifaces = os.networkInterfaces();
            let found = false;
            for (const name in ifaces) {
                const iface = ifaces[name];
                if (!iface) continue;
                for (const alias of iface) {
                    if (alias.family === "IPv4" && !alias.internal) {
                        ip = alias.address;
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
        } catch (e: any) {
            this.log.debug(`[Setup] Could not determine network interfaces: ${e.message}`);
        }

        const url = `http://${ip}:${port}`;
        await this.setStateAsync("info.dashboardUrl", url, true);
        this.log.info(`Dashboard reachable at: ${url}`);
    }

    private async onMessage(obj: ioBroker.Message): Promise<void> {
        if (!obj || typeof obj !== "object") return;

        if (obj.command === "generateJson") {
            try {
                const tree = await this.generateJsonTree();
                if (obj.callback) this.sendTo(obj.from, obj.command, tree, obj.callback);
            } catch (e: any) {
                if (obj.callback) this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
            }
        }
        else if (obj.command === "checkSync") {
            this.log.info("Manual sync test requested...");
            try {
                const result = await this.runRemoteSync(true);
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, {
                        success: result.success,
                        result: result.message
                    }, obj.callback);
                }
            } catch (e: any) {
                 if (obj.callback) this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
            }
        }
        else if (obj.callback) {
            this.sendTo(obj.from, obj.command, { error: "unknown command" }, obj.callback);
        }
    }

    // Eigene Objekte der Instanz. Reihenfolge wichtig: die Channel "info" und "config" zuerst,
    // denn jeder State braucht ein Elternobjekt (sonst lehnt die ioBroker-Strukturprüfung ab).
    // extendObject statt setObjectNotExists: so erhalten auch bestehende Installationen korrigierte
    // Definitionen (z.B. die Rolle von info.version, die bis 1.6.1 "info.version" lautete).
    private static readonly OWN_OBJECTS: { id: string; obj: ioBroker.SettableObject }[] = [
        { id: "info", obj: { type: "channel", common: { name: "Information" }, native: {} } },
        { id: "config", obj: { type: "channel", common: { name: "Configuration" }, native: {} } },
        { id: "watchdog", obj: { type: "state", common: { name: "MQTT Bridge Watchdog", type: "string", role: "text", read: true, write: false }, native: {} } },
        { id: "config.syncTemplate", obj: { type: "state", common: { name: "Remote Sync JSON Template", type: "string", role: "json", read: true, write: true }, native: {} } },
        { id: "info.dashboardUrl", obj: { type: "state", common: { name: "Dashboard URL", type: "string", role: "url", read: true, write: false }, native: {} } },
        { id: "info.lastSyncStatus", obj: { type: "state", common: { name: "Last sync status", type: "string", role: "text", read: true, write: false }, native: {} } },
        { id: "info.status", obj: { type: "state", common: { name: "Status", type: "string", role: "text", read: true, write: false }, native: {} } },
        { id: "info.lastCycle", obj: { type: "state", common: { name: "Last sync cycle", type: "number", role: "value.time", read: true, write: false }, native: {} } },
        { id: "info.connection", obj: { type: "state", common: { name: "Connected", type: "boolean", role: "indicator.connected", read: true, write: false, def: false }, native: {} } },
        { id: "info.version", obj: { type: "state", common: { name: "Adapter version", type: "string", role: "text", read: true, write: false }, native: {} } },
        { id: "info.authLockouts", obj: { type: "state", common: { name: "Login lockout list (internal)", type: "string", role: "json", read: true, write: false, def: "{}" }, native: {} } },
    ];

    private async initObjects(): Promise<void> {
        for (const { id, obj } of MqttPlus.OWN_OBJECTS) {
            const def = id === "config.syncTemplate"
                ? { ...obj, common: { ...obj.common, def: this.getDefaultSyncTemplate() } } as ioBroker.SettableObject
                : obj;
            await this.extendObject(id, def);
        }
    }

    // Lädt aktive Login-Sperren aus der Persistenz, damit ein Adapter-Neustart eine laufende
    // Brute-Force-Sperre nicht zurücksetzt. Bereits abgelaufene Einträge werden verworfen.
    private async loadAuthLockouts(): Promise<void> {
        try {
            const state = await this.getStateAsync("info.authLockouts");
            if (!state || !state.val) return;
            const stored = JSON.parse(state.val as string) as Record<string, { count: number; lockedUntil: number }>;
            const now = Date.now();
            for (const [ip, entry] of Object.entries(stored)) {
                if (entry.lockedUntil > now) this.failedAuthAttempts.set(ip, entry);
            }
        } catch (e: any) {
            this.log.debug(`[Dashboard] Could not load lockout list: ${e.message}`);
        }
    }

    // Persistiert nur bei tatsächlicher Sperrung (nicht bei jedem Fehlversuch), um die
    // States-DB nicht unnötig zu belasten - und räumt dabei abgelaufene Einträge auf.
    private persistAuthLockouts(): void {
        const now = Date.now();
        const active: Record<string, { count: number; lockedUntil: number }> = {};
        for (const [ip, entry] of this.failedAuthAttempts) {
            if (entry.lockedUntil > now) active[ip] = entry;
        }
        this.setState("info.authLockouts", JSON.stringify(active), true);
    }

    private async setupBridge(): Promise<void> {
        this.log.info("[MQTT bridge] Starting setup & indexing...");

        this.sourceToMappings.clear();
        this.targetToMappings.clear();
        this.targetTypeCache.clear();
        this.sourceTypeCache.clear();

        const mappings = this.config.mappings || [];
        if (!mappings || !Array.isArray(mappings)) return;

        const basePath = this.getValidatedBasePath();
        const seenTargetPaths = new Map<string, string>();
        const subscribeIds: string[] = [];

        for (const entry of mappings) {
            if (!entry.id || !entry.mqttName) continue;

            const cleanSuffix = this.convertMqttPathToIobrokerId(entry.mqttName);
            const fullTargetPath = `${basePath}${cleanSuffix}`;
            const commandPath = this.resolveCommandPath(entry, fullTargetPath);
            entry.fullTargetPath = fullTargetPath;
            entry.commandPath = commandPath;

            try {
                const sObj = await this.getForeignObjectAsync(entry.id);
                const sType = sObj && sObj.common && sObj.common.type;
                this.sourceTypeCache.set(entry.id, sType || "unknown");
                // Auch als Ziel-Typ cachen: entry.id ist bei dir "in"/"both" ein Schreibziel, und
                // convertType() kannte dessen echten Typ bisher nur über fullTargetPath (IOB->MQTT-
                // Richtung) - die Befehlsrichtung (MQTT->IOB) bekam bei "auto" nie eine Koerzion.
                if (sType) {
                    this.targetTypeCache.set(entry.id, sType);
                }
            } catch (e: any) {
                this.log.debug(`[Setup] Source object ${entry.id} not readable: ${e.message}`);
                this.sourceTypeCache.set(entry.id, "unknown");
            }

            // Source Index (IOB -> MQTT)
            if (entry.dir === "out" || entry.dir === "both") {
                const prevOwner = seenTargetPaths.get(fullTargetPath);
                if (prevOwner && prevOwner !== entry.id) {
                    this.log.warn(`[Setup] Duplicate target topic "${fullTargetPath}": written by both "${prevOwner}" and "${entry.id}" - the values overwrite each other!`);
                } else {
                    seenTargetPaths.set(fullTargetPath, entry.id);
                }

                if (!this.sourceToMappings.has(entry.id)) {
                    this.sourceToMappings.set(entry.id, []);
                }
                this.sourceToMappings.get(entry.id)!.push(entry);
                subscribeIds.push(entry.id);
            }

            // Target Index (MQTT -> IOB): abonniert wird das Befehls-Topic. Bei "dual" ist das
            // <topic>/set, bei "single" das Basis-Topic selbst.
            if (entry.dir === "in" || entry.dir === "both") {
                if (!this.targetToMappings.has(commandPath)) {
                    this.targetToMappings.set(commandPath, []);
                }
                this.targetToMappings.get(commandPath)!.push(entry);
                subscribeIds.push(commandPath);
            }

            // Reihenfolge ist wichtig: erst das Status-Topic als State anlegen, danach das
            // darunterliegende Befehls-Topic. Andernfalls würde die Ordner-Anlage das Basis-Topic
            // als "folder" erzeugen, bevor es als State existiert.
            await this.ensureAdapterObject(entry.id, fullTargetPath, entry.unit);
            if (commandPath !== fullTargetPath) {
                await this.ensureAdapterObject(entry.id, commandPath, entry.unit);
            }
        }

        // Ein gebündelter Subscribe-Aufruf statt einem pro ID
        if (subscribeIds.length > 0) {
            await this.subscribeForeignStatesAsync(subscribeIds);
        }

        // Initial-Sync gebündelt: ein getForeignStatesAsync-Roundtrip statt N Einzelabfragen.
        // Zusätzlich werden die aktuellen Zielwerte gelesen: Ist der Wert dort schon korrekt,
        // wird nicht neu geschrieben - sonst bekäme jeder Spiegel nach jedem Neustart ein
        // frisches ts, obwohl sich nichts geändert hat (und ein totes Gerät sähe aktiv aus).
        const outIds = [...this.sourceToMappings.keys()];
        const outStates = outIds.length ? await this.getForeignStatesAsync(outIds) : {};
        const outTargetStates = await this.getTargetStates(outIds.flatMap(id => this.sourceToMappings.get(id)!.map(e => e.fullTargetPath!)));
        for (const [id, entries] of this.sourceToMappings) {
            const state = outStates[id];
            for (const entry of entries) {
                if (entry.dir === "out" || entry.dir === "both") {
                    if (!state || !this.passesAckFilter(entry, state)) continue;
                    const label = entry.dir === "both" ? "IOB -> MQTT (START: both)" : "IOB -> MQTT (START)";
                    await this.syncValue(id, entry.fullTargetPath!, label, "START-UP", entry.type, state,
                        this.syncOptionsFor(entry, "start", outTargetStates[entry.fullTargetPath!]));
                }
            }
        }

        // Achtung: Bei topicMode "dual" ist das Quell-Topic ein reiner BEFEHLS-Kanal, kein
        // Zustandsspeicher. Ein dort liegender (evtl. retained) Befehl darf beim Start nicht
        // erneut ausgeführt werden - sonst würde z.B. nach jedem ioBroker-Neustart das zuletzt
        // gesendete Kommando das Gerät erneut schalten. Deshalb nur "single" initial abgleichen.
        const inOnlyTargets: string[] = [];
        for (const [commandPath, entries] of this.targetToMappings) {
            if (entries.some(e => e.dir === "in" && e.topicMode !== "dual")) inOnlyTargets.push(commandPath);
        }
        const inStates = inOnlyTargets.length ? await this.getForeignStatesAsync(inOnlyTargets) : {};
        const inTargetStates = await this.getTargetStates(inOnlyTargets.flatMap(p => this.targetToMappings.get(p)!.map(e => e.id)));
        for (const [commandPath, entries] of this.targetToMappings) {
            const state = inStates[commandPath];
            for (const entry of entries) {
                if (entry.dir === "in" && entry.topicMode !== "dual") {
                    // Wie im Event-Pfad (onStateChange) nur echte Broker-Werte (ack=true) übernehmen.
                    if (!state || state.ack !== true) continue;
                    await this.syncValue(commandPath, entry.id, "MQTT -> IOB (START)", "START-UP", entry.type, state,
                        this.syncOptionsFor(entry, "start", inTargetStates[entry.id]));
                }
            }
        }

        this.log.info(`[Setup] Indexed: ${this.sourceToMappings.size} sources, ${this.targetToMappings.size} targets.`);
    }

    private async onStateChange(id: string, state: ioBroker.State | null | undefined): Promise<void> {
        if (!state) return;

        // 1. Ist es eine Quelle? (IOB -> MQTT)
        const sourceMatches = this.sourceToMappings.get(id);
        if (sourceMatches) {
            for (const entry of sourceMatches) {
                // Standard: nur bestätigte Werte (ack=true) weiterleiten. Für Datenpunkte ohne
                // echtes Gerät dahinter (0_userdata.0.*, alias.0.*) kann pro Mapping "any" gewählt
                // werden, damit auch ack=false (Admin-UI/Skripte) sofort statt erst beim nächsten
                // Cycle-Sync durchkommt.
                if (this.passesAckFilter(entry, state) && entry.fullTargetPath) {
                    const dirLabel = entry.dir === "both" ? "IOB -> MQTT (Config: both)" : "IOB -> MQTT";
                    // Echo-Schutz nur bei "any" aktivieren: nur dort können ack=false-Ereignisse
                    // (also potenziell unser eigener Schreibvorgang) diesen Zweig überhaupt erreichen.
                    // Keine Aktualitätsprüfung: das Ereignis selbst belegt, dass die Quelle lebt.
                    await this.syncValue(id, entry.fullTargetPath, dirLabel, "LOCAL-CHANGE", entry.type, state, {
                        ...this.syncOptionsFor(entry, "event"),
                        useEchoGuard: entry.ackFilter === "any"
                    });
                }
            }
        }

        // 2. Ist es ein Ziel? (MQTT -> IOB)
        const targetMatches = this.targetToMappings.get(id);
        if (targetMatches) {
            for (const entry of targetMatches) {
                if (!entry.commandPath) continue;

                // Die Ack-Prüfung ist nur im Modus "single" nötig - dort teilen sich Status und
                // Befehl ein Topic, und ein ack=false-Ereignis wäre unser eigener Schreibvorgang
                // (Echo). Bei "dual" schreiben wir auf das Befehls-Topic grundsätzlich nie, ein
                // Echo kann dort also gar nicht entstehen. Die Prüfung würde dort im Gegenteil
                // schaden: Übliche MQTT-Adapter reichen eingehende /set-Nachrichten bewusst als
                // unbestätigten Steuerbefehl (ack=false) weiter - der Schaltbefehl würde dann
                // stillschweigend verworfen.
                const isDual = entry.topicMode === "dual";
                if (!isDual && state.ack !== true) continue;

                const modeLabel = isDual ? " [dual]" : "";
                const dirLabel = entry.dir === "both" ? `MQTT -> IOB (Config: both${modeLabel})` : `MQTT -> IOB${modeLabel}`;
                // Bei "both" ist dies die Rückrichtung des konfigurierten Typs (z.B. boolToNum <-> numToBool).
                // Der Wert selbst wird dabei nie invertiert (true bleibt true), nur die Darstellung angepasst.
                const effectiveType = entry.dir === "both" ? this.invertConversionType(entry.type) : entry.type;
                // Bei "dual" ist das ein neuer Befehl - der bekommt bewusst den aktuellen Zeitpunkt,
                // nicht den der MQTT-Nachricht, und wird nie als "Refresh" wiederholt. Bei "single"
                // wird ein Zustand gespiegelt.
                const eventOpts: SyncOptions = isDual
                    ? { decimals: entry.decimals, preserveTimestamp: false, isCommand: true }
                    : this.syncOptionsFor(entry, "event");
                await this.syncValue(entry.commandPath, entry.id, dirLabel, "MQTT-EVENT", effectiveType, state, eventOpts);
            }
        }
    }

    /**
     * Sync Funktion. Gibt true zurück, wenn tatsächlich geschrieben wurde.
     */
    private async syncValue(
        sourceId: string,
        targetId: string,
        dirLabel: string,
        triggerSource: string,
        convType: string,
        stateObj: ioBroker.State | null | undefined,
        opts: SyncOptions = {}
    ): Promise<boolean> {
        const force = opts.force === true;
        try {
            let srcState: ioBroker.State | null | undefined = stateObj;
            if (!srcState) {
                srcState = await this.getForeignStateAsync(sourceId);
            }
            if (!srcState) return false;
            const val = srcState.val;

            if (val === null || val === undefined) return false;

            // --- 0. AKTUALITÄT DER QUELLE ---
            // Nur bei Start/Cycle/Force-Sync: dort liegt kein frisches Ereignis vor, der Wert kann
            // beliebig alt sein. Ein totes Gerät soll auf der Zielseite nicht "neu" werden.
            if (opts.requireAlive) {
                const staleReason = this.getStaleReason(srcState, opts.staleLimitMs ?? 0);
                if (staleReason) {
                    if (!this.staleSources.has(sourceId)) {
                        this.staleSources.add(sourceId);
                        this.log.info(`[Staleness] Source ${sourceId} is considered inactive (${staleReason}) - not mirrored to ${targetId} until its next real update.`);
                    } else {
                        this.log.debug(`[Staleness] (${triggerSource}) Skipping ${sourceId} -> ${targetId}: ${staleReason}`);
                    }
                    return false;
                }
            }
            if (this.staleSources.delete(sourceId)) {
                this.log.info(`[Staleness] Source ${sourceId} is active again.`);
            }

            // --- 1. ECHO-SCHUTZ (verbrauchbar, mit kurzer Verfallszeit) ---
            // Ein evtl. vorhandener Merker wird immer konsumiert (aufgeräumt), damit er nicht
            // später ein unabhängiges Ereignis mit zufällig demselben Wert blockiert - reagiert
            // wird darauf aber nur, wenn useEchoGuard aktiv ist.
            const pending = this.pendingWrites.get(sourceId);
            if (pending) {
                this.pendingWrites.delete(sourceId);
                const stillFresh = Date.now() - pending.ts < MqttPlus.PENDING_WRITE_TTL_MS;
                if (opts.useEchoGuard && stillFresh && this.sameValue(pending.value, val)) {
                    this.log.debug(`[Echo guard] (${triggerSource}) Ignoring echo ${sourceId} -> ${targetId} (value '${val}' equals own write)`);
                    return false;
                }
            }
            // ----------------------------------------------

            const processedValue = this.convertType(val, convType, targetId, opts.decimals ?? 2);
            // "::" statt "_to_": vermeidet Key-Kollisionen, falls eine ID selbst "_to_" enthält.
            const cacheKey = `${sourceId}::${targetId}`;

            // syncMode "refresh": eine neue Meldung der Quelle (neueres ts) mit gleichem Wert
            // zählt trotzdem als weiterzureichen - das Ziel bleibt so aktuell, solange das Gerät
            // tatsächlich meldet, und veraltet ehrlich, sobald es verstummt.
            const srcTs = typeof srcState.ts === "number" ? srcState.ts : 0;
            const isRefresh = (knownTs: number | undefined) => opts.passRefresh === true && srcTs > (knownTs ?? 0);

            // --- 2. VALUE CACHE (Ping-Pong Schutz für langsame Echos) ---
            // WICHTIG: Wenn force = true ist, ignorieren wir den Cache komplett!
            // Ebenso bei Befehlen (dual /set): Beobachtet am Shelly-Schalter - per Dashboard
            // eingeschaltet (Cache: true), am Gerät ausgeschaltet (läuft über die Gegenrichtung,
            // der Cache bleibt true), erneutes Einschalten per Dashboard wurde als "redundant"
            // verworfen. Erst aus und wieder ein half.
            if (!force && !opts.isCommand && this.sameValue(this.lastSyncValues.get(cacheKey), processedValue) && !isRefresh(this.lastSyncTs.get(cacheKey))) {
                this.log.debug(`[Cache guard] Blocking redundant value for ${targetId} (value '${processedValue}' equals the last sent value)`);
                return false;
            }

            // --- 2b. ZIELVERGLEICH (Start/Force-Sync) ---
            // Steht auf der Zielseite bereits der richtige Wert, gibt es nichts zu heilen. Ein
            // erneutes Schreiben würde nur ts/lc des Ziels auffrischen und eine MQTT-Nachricht
            // auslösen - und damit ein inaktives Gerät als aktiv erscheinen lassen.
            if (opts.targetState && this.sameValue(opts.targetState.val, processedValue) && !isRefresh(opts.targetState.ts)) {
                this.lastSyncValues.set(cacheKey, processedValue);
                this.lastSyncTs.set(cacheKey, srcTs);
                this.log.debug(`[Target guard] (${triggerSource}) ${targetId} already has the value '${processedValue}' - no write needed`);
                return false;
            }

            this.lastSyncValues.set(cacheKey, processedValue);
            this.lastSyncTs.set(cacheKey, srcTs);
            // -----------------------------------------------------------

            // --- 3. SCHREIBEN & ECHO-MERKER SETZEN ---
            this.pendingWrites.set(targetId, { value: processedValue, ts: Date.now() });

            // Spiegelung eines Zustands: Zeitstempel (ts = letzte Aktualisierung, lc = letzte
            // Änderung) und Qualität der Quelle übernehmen, damit das Ziel den echten
            // Messzeitpunkt zeigt statt des Kopier-Zeitpunkts. Befehle (dual /set) bekommen
            // dagegen den aktuellen Zeitpunkt - ein Befehl ist tatsächlich neu.
            const newState: ioBroker.SettableState = { val: processedValue, ack: false, c: "mqtt-plus" };
            if (opts.preserveTimestamp) {
                if (typeof srcState.ts === "number") newState.ts = srcState.ts;
                if (typeof srcState.lc === "number") newState.lc = srcState.lc;
                if (typeof srcState.q === "number") newState.q = srcState.q;
            }
            await this.setForeignStateAsync(targetId, newState);

            if (this.config.logTransfers) {
                const forceLabel = force ? "[FORCED] " : "";
                this.log.info(`${forceLabel}[${dirLabel}] (${triggerSource}) ${val} -> ${processedValue} (${targetId})`);
            }
            this.updateWatchdog("Running");
            return true;
        } catch (e: any) {
            this.log.error(`Sync error ${sourceId}: ${e.message}`);
            return false;
        }
    }

    // Standard: nur bestätigte Werte (ack=true) weiterleiten. Für Datenpunkte ohne echtes Gerät
    // dahinter (0_userdata.0.*, alias.0.*) kann pro Mapping "any" gewählt werden. Gilt für alle
    // Wege IOB -> MQTT (Event, Start, Cycle, Force) - sonst würde z.B. der Cycle einen nie
    // bestätigten Befehl an ein Offline-Gerät nach MQTT als Status melden.
    private passesAckFilter(entry: MappingEntry, state: ioBroker.State): boolean {
        return entry.ackFilter === "any" || state.ack === true;
    }

    // Sync-Optionen eines Mappings je Auslöser, abhängig vom Sync-Modus des Eintrags.
    private syncOptionsFor(entry: MappingEntry, phase: SyncPhase, targetState?: ioBroker.State | null): SyncOptions {
        if (entry.syncMode === "force") {
            // Verhalten vor 1.6.0: keine Aktualitätsprüfung, kein Zielvergleich, ts = jetzt.
            // Start und Force-Sync schreiben dadurch immer - das Ziel wirkt dauerhaft frisch.
            return { force: phase === "force", decimals: entry.decimals, preserveTimestamp: false };
        }
        const opts: SyncOptions = {
            decimals: entry.decimals,
            preserveTimestamp: true,
            passRefresh: entry.syncMode === "refresh"
        };
        if (phase === "event") return opts;
        opts.requireAlive = true;
        opts.staleLimitMs = this.getStaleLimitMs(entry);
        if (phase === "force") opts.force = true;
        if (phase === "start" || phase === "force") opts.targetState = targetState ?? null;
        return opts;
    }

    // Liest eine Minuten-Angabe aus der Config tolerant ein: leer/ungültig -> undefined.
    private parseMinutes(raw: any): number | undefined {
        if (raw === undefined || raw === null || String(raw).trim() === "") return undefined;
        const n = Number(raw);
        return isNaN(n) || n < 0 ? undefined : n;
    }

    // Aktualitätsgrenze in ms für ein Mapping: Eintrag im Mapping vor globalem Wert, 0 = aus.
    private getStaleLimitMs(entry: MappingEntry): number {
        const perMapping = this.parseMinutes(entry.staleAfterMin);
        const global = this.parseMinutes(this.config.staleAfterMin) ?? MqttPlus.DEFAULT_STALE_AFTER_MIN;
        return (perMapping ?? global) * 60_000;
    }

    // Liefert den Grund, warum eine Quelle als inaktiv gilt, oder null wenn sie aktiv ist.
    // - q != 0: der Geräte-Adapter meldet selbst ein Problem (z.B. keine Verbindung).
    // - ts älter als die Grenze: das Gerät hat sich seitdem nicht mehr gemeldet.
    private getStaleReason(state: ioBroker.State, staleLimitMs: number): string | null {
        if (typeof state.q === "number" && state.q !== 0) {
            return `quality q=0x${state.q.toString(16).padStart(2, "0")}`;
        }
        if (staleLimitMs > 0 && typeof state.ts === "number") {
            const age = Date.now() - state.ts;
            if (age > staleLimitMs) return `last update ${this.formatAge(age)} ago`;
        }
        return null;
    }

    private formatAge(ms: number): string {
        const min = Math.round(ms / 60_000);
        if (min < 120) return `${min} min`;
        const h = Math.round(min / 60);
        if (h < 48) return `${h} h`;
        return `${Math.round(h / 24)} days`;
    }

    // Liest die aktuellen Werte der Zielobjekte gebündelt (für den Zielvergleich bei Start/Force).
    private async getTargetStates(ids: string[]): Promise<Record<string, ioBroker.State | null | undefined>> {
        const unique = [...new Set(ids.filter(Boolean))];
        if (!unique.length) return {};
        try {
            return await this.getForeignStatesAsync(unique);
        } catch (e: any) {
            this.log.debug(`[Sync] Target values not readable, writing without comparison: ${e.message}`);
            return {};
        }
    }

    // Kehrt boolToNum/numToBool für die Rückrichtung eines "both"-Mappings um.
    // round/auto sind richtungsunabhängig symmetrisch und bleiben unverändert.
    private invertConversionType(type: string): string {
        switch (type) {
            case "boolToNum": return "numToBool";
            case "numToBool": return "boolToNum";
            default: return type;
        }
    }

    // Wandelt einen Wert korrekt in boolean um - Strings wie "0"/"false"/"off" zählen als false,
    // statt (wie !!value) fälschlich als true.
    private toBoolean(value: any): boolean {
        if (typeof value === "string") {
            const normalized = value.trim().toLowerCase();
            if (normalized === "0" || normalized === "false" || normalized === "off" || normalized === "") return false;
            if (normalized === "1" || normalized === "true" || normalized === "on") return true;
        }
        return !!value;
    }

    // Typtoleranter Vergleich für Echo-/Cache-Erkennung: true/"true", 53/"53" gelten als gleich.
    // Ein reiner === schlägt fehl, wenn derselbe Wert einmal als String, einmal als Zahl/Boolean
    // vorliegt, obwohl er inhaltlich identisch ist - mit Folgen in beide Richtungen (verpasste
    // Echo-Erkennung oder verpasste Cache-Treffer).
    private sameValue(a: any, b: any): boolean {
        if (a === b) return true;
        if (a === null || a === undefined || b === null || b === undefined) return false;

        const numA = typeof a === "number" ? a : (typeof a === "string" && a.trim() !== "" && !isNaN(Number(a)) ? Number(a) : null);
        const numB = typeof b === "number" ? b : (typeof b === "string" && b.trim() !== "" && !isNaN(Number(b)) ? Number(b) : null);
        if (numA !== null && numB !== null) return numA === numB;

        const isBoolLike = (v: any) => typeof v === "boolean" || (typeof v === "string" && ["true", "false"].includes(v.trim().toLowerCase()));
        if (isBoolLike(a) && isBoolLike(b)) return this.toBoolean(a) === this.toBoolean(b);

        return String(a) === String(b);
    }

    private convertType(value: any, targetType: string, targetId: string, decimals: number = 2): any {
        if (value === null || value === undefined) return value;

        switch (targetType) {
            case "boolToNum":
                return value ? 1 : 0;
            case "numToBool":
                return this.toBoolean(value);
            case "round": {
                const factor = Math.pow(10, decimals);
                if (typeof value === "number") return Math.round(value * factor) / factor;
                const num = parseFloat(value);
                return isNaN(num) ? value : Math.round(num * factor) / factor;
            }
            default: {
                // Ohne explizit konfigurierten Typ: anhand des tatsächlichen Zielobjekt-Typs
                // (aus ensureAdapterObject bekannt) automatisch konvertieren statt anhand einer
                // fragilen Namensheuristik.
                const knownType = this.targetTypeCache.get(targetId);
                if (knownType === "boolean") return this.toBoolean(value);
                if (knownType === "number") {
                    const num = typeof value === "number" ? value : parseFloat(value);
                    return isNaN(num) ? value : num;
                }
                return value;
            }
        }
    }

    // Wartet aktiv (statt eines pauschalen Sleeps) darauf, dass ein neu angelegtes Objekt
    // in der Objekt-DB sichtbar ist - beendet sich sobald es existiert, spätestens nach timeoutMs.
    private async waitForObject(id: string, timeoutMs = 2000): Promise<void> {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const obj = await this.getForeignObjectAsync(id);
            if (obj) return;
            await this.sleep(100);
        }
    }

    private async ensureAdapterObject(sourceId: string, targetPath: string, mappingUnit?: string): Promise<void> {
        const parts = targetPath.split(".");
        if (parts.length < 2) {
            this.log.warn(`[Setup] Invalid target path "${targetPath}" (base path too short) - skipped.`);
            return;
        }
        let currentPath = parts[0] + "." + parts[1];

        for (let i = 2; i < parts.length - 1; i++) {
            currentPath += "." + parts[i];
            try {
                const exists = await this.getForeignObjectAsync(currentPath);
                if (!exists) {
                    await this.setForeignObjectAsync(currentPath, {
                        _id: currentPath,
                        type: "folder",
                        common: { name: parts[i] },
                        native: {}
                    });
                }
            } catch (e: any) {
                this.log.debug(`[Setup] Folder creation ${currentPath} skipped: ${e.message}`);
            }
        }

        try {
            const finalExists = await this.getForeignObjectAsync(targetPath);
            if (!finalExists) {
                const sObj = await this.getForeignObjectAsync(sourceId);
                let type: ioBroker.CommonType = "mixed";
                let role = "variable";
                let unit = mappingUnit;

                if (sObj && sObj.common) {
                    type = sObj.common.type || "mixed";
                    role = sObj.common.role || "variable";
                    if (!unit) unit = sObj.common.unit;
                }

                if (targetPath.includes("Switch") || targetPath.includes("Schalter")) {
                    type = "boolean";
                    role = "switch";
                }

                await this.setForeignObjectAsync(targetPath, {
                    _id: targetPath,
                    type: "state",
                    common: {
                        name: `Export of ${sourceId}`,
                        type: type,
                        role: role,
                        unit: unit,
                        read: true,
                        write: true
                    },
                    native: {}
                });

                this.targetTypeCache.set(targetPath, type);
                await this.waitForObject(targetPath);
            } else if (finalExists.common) {
                this.targetTypeCache.set(targetPath, finalExists.common.type || "mixed");
            }
        } catch (e: any) {
            this.log.debug(`[Setup] Target object ${targetPath} skipped: ${e.message}`);
        }
    }

    /**
     * Regulärer Cycle Sync: Prüft mit Cache (produziert keinen massiven Funkverkehr)
     */
    private async runCycleSync(): Promise<void> {
        if (this.syncRunning || this.unloaded) return;
        this.syncRunning = true;
        try {
            const ids = [...this.sourceToMappings.keys()];
            const states = ids.length ? await this.getForeignStatesAsync(ids) : {};
            for (const [id, entries] of this.sourceToMappings) {
                const state = states[id];
                if (!state) continue;
                for (const entry of entries) {
                    if (entry.fullTargetPath && (entry.dir === "out" || entry.dir === "both")) {
                        if (!this.passesAckFilter(entry, state)) continue;
                        const label = entry.dir === "both" ? "IOB -> MQTT (CYCLE: both)" : "IOB -> MQTT (CYCLE)";
                        await this.syncValue(id, entry.fullTargetPath, label, "CYCLE", entry.type, state, this.syncOptionsFor(entry, "cycle"));
                    }
                }
            }
            this.updateWatchdog("Cycle OK", true);
        } catch (e: any) {
            this.log.error(`Cycle sync error: ${e.message}`);
        } finally {
            this.syncRunning = false;
        }
    }

    /**
     * Force Sync (Heilung): Ignoriert den Cache und vergleicht direkt mit dem tatsächlichen
     * Zielwert - geschrieben wird nur, wo Quelle und Ziel wirklich auseinanderlaufen, und nur
     * von aktiven Quellen. Läuft nie parallel zum Cycle-Sync (gemeinsame Sperre) und staffelt
     * die Schreibvorgänge, um Funkbudget (Zigbee/433MHz) nicht als Burst zu belasten.
     */
    private async runForceSync(): Promise<void> {
        if (this.syncRunning || this.unloaded) return;
        this.syncRunning = true;
        try {
            this.log.info("[Force sync] Starting periodic forced synchronisation (heals out-of-sync states)...");
            let healed = 0;
            let forced = 0;

            // 1. IOB -> MQTT (für 'out' und 'both' - IOB ist die Quelle der Wahrheit für Aktoren)
            const outIds = [...this.sourceToMappings.keys()];
            const outStates = outIds.length ? await this.getForeignStatesAsync(outIds) : {};
            const outTargetStates = await this.getTargetStates(outIds.flatMap(id => this.sourceToMappings.get(id)!.map(e => e.fullTargetPath!)));
            for (const [id, entries] of this.sourceToMappings) {
                const state = outStates[id];
                if (!state) continue;
                for (const entry of entries) {
                    if (entry.fullTargetPath && (entry.dir === "out" || entry.dir === "both")) {
                        if (!this.passesAckFilter(entry, state)) continue;
                        const label = entry.dir === "both" ? "IOB -> MQTT (FORCE: both)" : "IOB -> MQTT (FORCE)";
                        const written = await this.syncValue(id, entry.fullTargetPath, label, "FORCE-SYNC", entry.type, state,
                            this.syncOptionsFor(entry, "force", outTargetStates[entry.fullTargetPath]));
                        if (written) {
                            if (entry.syncMode === "force") forced++; else healed++;
                            await this.sleep(75);
                        }
                    }
                }
                if (this.unloaded) return;
            }

            // 2. MQTT -> IOB (für 'in' - MQTT ist die Quelle der Wahrheit für externe Sensoren)
            // Nur "single": bei "dual" ist die Quelle ein Befehls-Topic - ein Force-Sync würde
            // dort den zuletzt gesendeten Befehl periodisch wiederholen und damit z.B. ein am
            // Wandschalter ausgeschaltetes Licht von selbst wieder einschalten.
            const inIds: string[] = [];
            for (const [commandPath, entries] of this.targetToMappings) {
                if (entries.some(e => e.dir === "in" && e.topicMode !== "dual")) inIds.push(commandPath);
            }
            const inStates = inIds.length ? await this.getForeignStatesAsync(inIds) : {};
            const inTargetStates = await this.getTargetStates(inIds.flatMap(p => this.targetToMappings.get(p)!.map(e => e.id)));
            for (const [commandPath, entries] of this.targetToMappings) {
                const state = inStates[commandPath];
                // Wie im Event-Pfad nur echte Broker-Werte (ack=true) übernehmen.
                if (!state || state.ack !== true) continue;
                for (const entry of entries) {
                    if (entry.dir === "in" && entry.topicMode !== "dual") {
                        const written = await this.syncValue(commandPath, entry.id, "MQTT -> IOB (FORCE: in)", "FORCE-SYNC", entry.type, state,
                            this.syncOptionsFor(entry, "force", inTargetStates[entry.id]));
                        if (written) {
                            if (entry.syncMode === "force") forced++; else healed++;
                            await this.sleep(75);
                        }
                    }
                }
                if (this.unloaded) return;
            }

            const forcedInfo = forced ? `, ${forced} rewritten in mode "Force"` : "";
            this.log.info(`[Force sync] Finished: ${healed} deviating values healed${forcedInfo}.`);
            this.updateWatchdog("Force-Sync OK", true);
        } catch (e: any) {
            this.log.error(`Force sync error: ${e.message}`);
        } finally {
            this.syncRunning = false;
        }
    }

    private async generateJsonTree(): Promise<Record<string, any>> {
        const mappings = this.config.mappings || [];
        const tree: Record<string, any> = {};
        const basePath = this.getValidatedBasePath();

        for (const entry of mappings) {
            if (!entry.mqttName) continue;

            const cleanSuffix = this.convertMqttPathToIobrokerId(entry.mqttName);
            const pathParts = cleanSuffix.split(".");

            let current = tree;
            for (let i = 0; i < pathParts.length; i++) {
                const part = pathParts[i];
                const isLast = i === pathParts.length - 1;

                if (isLast) {
                    if (current[part] && typeof current[part] === "object" && !current[part].full_topic) {
                        this.log.warn(`[JSON tree] Topic prefix collision: "${cleanSuffix}" overwrites an existing substructure.`);
                    }
                    const statePath = `${basePath}${cleanSuffix}`;
                    const commandPath = this.resolveCommandPath(entry, statePath);
                    current[part] = {
                        full_topic: statePath,
                        // Bei "dual" laufen Befehle über ein eigenes Topic - im Export sichtbar,
                        // damit Backup und Vorschau die tatsächliche Topic-Struktur abbilden.
                        command_topic: commandPath,
                        topic_mode: entry.topicMode === "dual" ? "dual" : "single",
                        iobroker_id: entry.id,
                        type: this.sourceTypeCache.get(entry.id) || "unknown",
                        unit: entry.unit || ""
                    };
                } else {
                    if (current[part] && current[part].full_topic) {
                        this.log.warn(`[JSON tree] Topic prefix collision: "${cleanSuffix}" collides with the existing topic "${current[part].full_topic}".`);
                        current[part] = {};
                    } else if (!current[part]) {
                        current[part] = {};
                    }
                    current = current[part];
                }
            }
        }
        return tree;
    }

    private chunkArray<T>(arr: T[], size: number): T[][] {
        const out: T[][] = [];
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
        return out;
    }

    // Fehler, bei denen ein Retry mit identischer Payload nie zu einem anderen Ergebnis führt:
    // 4xx-Antworten (Client-/Konfigurationsfehler) und TLS-Zertifikatsfehler. Nur bei Netzwerk-
    // problemen (keine Antwort) oder 5xx-Serverfehlern lohnt sich ein erneuter Versuch.
    private isRetryableError(e: any): boolean {
        if (e.response) {
            return e.response.status >= 500;
        }
        const nonRetryableCodes = [
            "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
            "DEPTH_ZERO_SELF_SIGNED_CERT",
            "SELF_SIGNED_CERT_IN_CHAIN",
            "ERR_TLS_CERT_ALTNAME_INVALID",
            "CERT_HAS_EXPIRED"
        ];
        if (e.code && nonRetryableCodes.includes(e.code)) return false;
        return true;
    }

    private async postWithRetry(url: string, data: any, httpsAgent: https.Agent | undefined, retries = 2): Promise<void> {
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                await axios.post(url, data, {
                    timeout: 10000,
                    httpsAgent,
                    // Verhindert, dass eine gesetzte HTTPS_PROXY-Umgebungsvariable den eigenen
                    // httpsAgent (und damit die konfigurierte CA) stillschweigend umgeht.
                    proxy: false,
                    headers: {
                        "Content-Type": "application/json",
                        "User-Agent": `ioBroker.mqtt-plus/${ADAPTER_VERSION}`
                    }
                });
                return;
            } catch (e: any) {
                if (attempt === retries || !this.isRetryableError(e)) throw e;
                await this.sleep(500 * Math.pow(2, attempt));
            }
        }
    }

    // Repariert häufige Copy-Paste-Beschädigungen in eingefügten PEM-Zertifikaten:
    // 1) Editoren/Autokorrektur ersetzen normale Bindestriche in "-----BEGIN...-----" durch
    //    optisch identische Unicode-Striche (En-/Em-Dash, Minuszeichen, U+2010-U+2015, U+2212)
    //    oder normale Leerzeichen durch geschützte Leerzeichen (U+00A0).
    // 2) Manche einzeiligen Formularfelder wandeln echte Zeilenumbrüche in Leerzeichen um -
    //    dann fehlt der Zeilenumbruch nach BEGIN/vor END, den OpenSSLs PEM-Parser zwingend
    //    braucht ("no start line"), obwohl Zeichenzahl und Kopfzeile unauffällig aussehen.
    private normalizePem(pemInput: string): string {
        let pem = pemInput
            .replace(/[\u2010-\u2015\u2212]/g, "-")
            .replace(/\u00a0/g, " ")
            .replace(/\r\n/g, "\n");

        pem = pem.replace(/-----BEGIN ([A-Z ]+)-----\s*/g, "-----BEGIN $1-----\n");
        pem = pem.replace(/\s*-----END ([A-Z ]+)-----/g, "\n-----END $1-----");

        return pem;
    }


    private async runRemoteSync(verbose: boolean = false): Promise<{success: boolean, message: string}> {
        if (!this.config.syncUrl) {
             const msg = "No sync URL configured";
             await this.setStateAsync("info.lastSyncStatus", msg, true);
             return { success: false, message: msg };
        }

        let parsedUrl: URL;
        try {
            parsedUrl = new URL(this.config.syncUrl);
        } catch {
            const msg = "Invalid sync URL";
            this.log.error(`Remote Sync Error: ${msg} (${this.config.syncUrl})`);
            await this.setStateAsync("info.lastSyncStatus", msg, true);
            return { success: false, message: msg };
        }
        const encodedUrl = parsedUrl.toString();

        let templateStr = this.getDefaultSyncTemplate();
        try {
            const tplState = await this.getStateAsync("config.syncTemplate");
            if (tplState && tplState.val) templateStr = tplState.val as string;
        } catch (e: any) {
            this.log.debug(`[Remote Sync] Template state not readable, using default: ${e.message}`);
        }

        // Platzhalter, die roh in einen JSON-String eingesetzt werden, müssen JSON-escaped werden -
        // sonst bricht ein Anführungszeichen/Backslash in einer ID das gesamte JSON.
        const esc = (s: string) => JSON.stringify(String(s)).slice(1, -1);

        const mappings = this.config.mappings || [];
        const payload: any[] = [];
        let skippedStale = 0;

        for (const entry of mappings) {
            try {
                const state = await this.getForeignStateAsync(entry.id);
                if (state) {
                    if (this.config.remoteSyncSkipStale && this.getStaleReason(state, this.getStaleLimitMs(entry))) {
                        skippedStale++;
                        continue;
                    }
                    // Funktions-Ersetzer statt String: ein "$" im Wert würde sonst als
                    // Ersetzungsmuster ($&, $1 ...) interpretiert.
                    let itemStr = templateStr
                        .replace(/%ID%/g, () => esc(entry.id))
                        .replace(/%MQTT%/g, () => esc(entry.mqttName))
                        .replace(/%PREFIX%/g, () => esc(this.config.targetBasePath))
                        .replace(/%DIR%/g, () => esc(entry.dir))
                        .replace(/%VAL%/g, () => JSON.stringify(state.val))
                        .replace(/%TS%/g, () => String(state.ts))
                        .replace(/%LC%/g, () => String(state.lc ?? state.ts))
                        .replace(/%ACK%/g, () => String(state.ack === true))
                        .replace(/%Q%/g, () => String(state.q ?? 0))
                        .replace(/%UNIT%/g, () => esc(entry.unit || ""));

                    try {
                        payload.push(JSON.parse(itemStr));
                    } catch {
                         this.log.warn(`Remote sync template error for ${entry.id}`);
                    }
                }
            } catch (e: any) {
                this.log.debug(`[Remote Sync] State of ${entry.id} not readable: ${e.message}`);
            }
        }

        this.log.debug(`[Remote Sync] Payload to send: ${JSON.stringify(payload)}`);

        // Für interne/selbstsignierte Ziele: das eigene Zertifikat/CA gezielt vertrauen,
        // statt die Prüfung komplett abzuschalten. Ohne Angabe gilt der normale
        // System-Vertrauensstore (öffentliche CAs).
        let httpsAgent: https.Agent | undefined;
        if (this.config.syncCaCert && this.config.syncCaCert.trim()) {
            const normalizedCaCert = this.normalizePem(this.config.syncCaCert);

            try {
                httpsAgent = new https.Agent({ ca: normalizedCaCert });
                // Nur beim manuellen Verbindungstest (info-Level), um den Log bei regulären
                // Intervall-Läufen nicht zuzumüllen - beweist, dass die konfigurierte CA den
                // Adapter-Prozess tatsächlich erreicht hat (Diagnose für Config-Save/Restart-Probleme).
                if (verbose) {
                    let fingerprintInfo = "fingerprint not determinable";
                    try {
                        const cert = new crypto.X509Certificate(normalizedCaCert);
                        fingerprintInfo = `Subject="${cert.subject.replace(/\n/g, ", ")}" Fingerprint(SHA256)=${cert.fingerprint256}`;
                    } catch (certErr: any) {
                        const trimmed = normalizedCaCert.trim();
                        const headCodes = [...trimmed.slice(0, 12)].map(c => c.charCodeAt(0)).join(",");
                        fingerprintInfo = `Certificate could not be parsed even after normalisation: ${certErr.message} | first 12 char codes: ${headCodes} (expected for "-----BEGIN": 45,45,45,45,45,66,69,71,73,78,32,67)`;
                    }
                    this.log.info(`[Remote Sync] Custom CA loaded (${normalizedCaCert.trim().length} characters). ${fingerprintInfo}`);
                }
            } catch (e: any) {
                const msg = `Invalid CA certificate in the settings: ${e.message}`;
                this.log.error(`Remote Sync Error: ${msg}`);
                await this.setStateAsync("info.lastSyncStatus", msg, true);
                return { success: false, message: msg };
            }
        } else if (verbose) {
            this.log.info("[Remote Sync] No CA certificate configured - using the default system trust store.");
        }

        let sentCount = 0;
        try {
            // Sehr große Konfigurationen in Häppchen senden statt als einen Riesen-POST,
            // dessen einzelner Timeout sonst die komplette Payload verwirft.
            const chunks = this.chunkArray(payload, MqttPlus.REMOTE_SYNC_CHUNK_SIZE);
            for (const chunk of chunks) {
                await this.postWithRetry(encodedUrl, chunk, httpsAgent);
                sentCount += chunk.length;
            }

            const staleInfo = skippedStale ? `, ${skippedStale} inactive skipped` : "";
            const msg = `OK: ${payload.length} values sent${staleInfo} (${new Date().toLocaleTimeString()})`;
            await this.setStateAsync("info.lastSyncStatus", msg, true);
            this.log.debug(`Remote Sync (${payload.length}) OK.`);

            return { success: true, message: msg };
        } catch (e: any) {
            let errorMsg = e.message;
            if (e.response) {
                errorMsg = `HTTP ${e.response.status}: ${e.response.statusText}`;
            }
            if (e.code) {
                errorMsg += ` (${e.code})`;
            }

            // Bei Chunking zeigt der Teilerfolg, ob nur ein Bruchteil oder praktisch nichts
            // angekommen ist - relevant, weil ein einzelner gescheiterter Chunk sonst wie ein
            // Totalausfall aussieht, obwohl der Großteil der Werte bereits übertragen wurde.
            const progress = payload.length > MqttPlus.REMOTE_SYNC_CHUNK_SIZE ? ` (${sentCount}/${payload.length} values transmitted)` : "";
            const fullMsg = `Error${progress}: ${errorMsg} (${new Date().toLocaleTimeString()})`;

            this.log.error(`Remote Sync Error: ${fullMsg} | URL: ${encodedUrl}`);
            await this.setStateAsync("info.lastSyncStatus", fullMsg, true);

            return { success: false, message: fullMsg };
        }
    }

    // Konstante-Zeit-Vergleich gegen Timing-Angriffe auf den Passwortvergleich.
    private timingSafeStringEqual(a: string, b: string): boolean {
        const bufA = Buffer.from(a);
        const bufB = Buffer.from(b);
        if (bufA.length !== bufB.length) {
            // Trotzdem konstante Zeit vergleichen (gegen Längen-Rückschlüsse per Timing);
            // Ergebnis ist ohnehin false.
            crypto.timingSafeEqual(bufA, bufA);
            return false;
        }
        return crypto.timingSafeEqual(bufA, bufB);
    }

    // Vergleicht die "Authorization: Basic ..."-Kopfzeile gegen die konfigurierten Zugangsdaten.
    // Ist kein Passwort konfiguriert, bleibt der Server bewusst offen (Warnung erfolgt in onReady).
    // Zusätzlich: Brute-Force-Sperre nach zu vielen Fehlversuchen pro Client-IP.
    private checkAuth(req: http.IncomingMessage): boolean {
        const password = this.config.dashboardPassword;
        if (!password) return true;

        const ip = req.socket.remoteAddress || "unknown";
        const entry = this.failedAuthAttempts.get(ip);
        if (entry && entry.lockedUntil > Date.now()) return false;

        const user = this.config.dashboardUser || "admin";
        const header = req.headers["authorization"];
        let ok = false;

        if (header && header.startsWith("Basic ")) {
            try {
                const decoded = Buffer.from(header.slice(6), "base64").toString("utf8");
                const sep = decoded.indexOf(":");
                if (sep !== -1) {
                    const reqUser = decoded.slice(0, sep);
                    const reqPass = decoded.slice(sep + 1);
                    ok = this.timingSafeStringEqual(reqUser, user) && this.timingSafeStringEqual(reqPass, password);
                }
            } catch {
                ok = false;
            }
        }

        if (ok) {
            this.failedAuthAttempts.delete(ip);
            return true;
        }

        const attempts = (entry?.count || 0) + 1;
        if (attempts >= MqttPlus.MAX_AUTH_ATTEMPTS) {
            this.failedAuthAttempts.set(ip, { count: 0, lockedUntil: Date.now() + MqttPlus.AUTH_LOCKOUT_MS });
            this.log.warn(`[Dashboard] Too many failed login attempts from ${ip} - locked for 5 minutes.`);
            this.persistAuthLockouts();
        } else {
            this.failedAuthAttempts.set(ip, { count: attempts, lockedUntil: 0 });
        }
        return false;
    }

    // Vergleicht Origin/Referer-Header gegen den eigenen Host - Basis für CORS-Beschränkung
    // und CSRF-Schutz bei zustandsändernden Requests. Ohne Origin/Referer (z.B. curl/Skript,
    // kein Browser) wird nicht blockiert - dort besteht kein CSRF-Risiko über den Browser.
    private isSameOrigin(req: http.IncomingMessage): boolean {
        const host = req.headers.host;
        if (!host) return false;
        const check = (req.headers.origin || req.headers.referer) as string | undefined;
        if (!check) return true;
        try {
            return new URL(check).host === host;
        } catch {
            return false;
        }
    }

    // Liest den Request-Body byte-genau ein (Buffer statt String-Konkatenation, damit ein über
    // zwei Chunks gesplittetes Multibyte-Zeichen nicht zu korruptem JSON führt), bricht mit 413
    // ab, sobald maxBytes überschritten wird, und hängt bei Verbindungsabbruch/Timeout nicht.
    private readBodyLimited(req: http.IncomingMessage, res: http.ServerResponse, maxBytes: number): Promise<string | null> {
        return new Promise(resolve => {
            const chunks: Buffer[] = [];
            let size = 0;
            let settled = false;

            const finish = (result: string | null) => {
                if (settled) return;
                settled = true;
                resolve(result);
            };

            req.setTimeout(30000, () => {
                if (!settled) {
                    res.writeHead(408, { "Content-Type": "application/json" });
                    res.end(JSON.stringify({ success: false, error: "Timeout while receiving" }));
                    req.destroy();
                    finish(null);
                }
            });

            req.on("data", (chunk: Buffer) => {
                if (settled) return;
                size += chunk.length;
                if (size > maxBytes) {
                    res.writeHead(413, { "Content-Type": "application/json" });
                    res.end(JSON.stringify({ success: false, error: "Request too large" }));
                    req.destroy();
                    finish(null);
                    return;
                }
                chunks.push(chunk);
            });

            req.on("error", () => finish(null));

            req.on("end", () => {
                if (!settled) finish(Buffer.concat(chunks).toString("utf8"));
            });
        });
    }

    private startWebServer(port: number): void {
        try {
            const requestListener = async (req: http.IncomingMessage, res: http.ServerResponse): Promise<void> => {
                try {
                    res.setHeader("Access-Control-Allow-Methods", "GET, POST");

                    // CORS gezielt statt "*": nur die eigene Origin darf die Antwort lesen.
                    const sameOrigin = this.isSameOrigin(req);
                    if (sameOrigin && req.headers.origin) {
                        res.setHeader("Access-Control-Allow-Origin", req.headers.origin);
                    }

                    if (!this.checkAuth(req)) {
                        res.setHeader("WWW-Authenticate", 'Basic realm="mqtt-plus"');
                        res.writeHead(401, { "Content-Type": "text/plain; charset=utf-8" });
                        res.end("Unauthorized");
                        return;
                    }

                    // CSRF-Schutz: zustandsändernde Requests nur, wenn Origin/Referer zum eigenen
                    // Host passt. Basic-Auth wird vom Browser automatisch mitgeschickt - ohne
                    // diese Prüfung könnte jede fremde Webseite die Mapping-Konfiguration
                    // überschreiben.
                    if (req.method === "POST" && !sameOrigin) {
                        res.writeHead(403, { "Content-Type": "application/json" });
                        res.end(JSON.stringify({ success: false, error: "Cross-origin request rejected" }));
                        return;
                    }

                    // Pfad statt rohem req.url vergleichen - sonst bricht jeder Query-String (?t=123)
                    const pathname = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`).pathname;

                    if (pathname === "/" || pathname === "/index.html") {
                        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                        res.end(this.getDashboardHtml());
                    }
                    else if (pathname === "/api/json") {
                        const tree = await this.generateJsonTree();
                        const exportData = {
                            prefix: this.config.targetBasePath,
                            mappings: this.config.mappings || [],
                            structure: tree
                        };
                        res.writeHead(200, { "Content-Type": "application/json" });
                        res.end(JSON.stringify(exportData, null, 2));
                    }
                    else if (pathname === "/api/status") {
                        const syncTemplateState = await this.getStateAsync("config.syncTemplate");
                        const status = {
                            watchdog: this.currentWatchdogStatus,
                            uptime: process.uptime(),
                            mappings: this.config.mappings ? this.config.mappings.length : 0,
                            syncTemplate: syncTemplateState ? syncTemplateState.val : this.getDefaultSyncTemplate()
                        };
                        res.writeHead(200, { "Content-Type": "application/json" });
                        res.end(JSON.stringify(status));
                    }
                    else if (pathname === "/api/save-template" && req.method === "POST") {
                        const body = await this.readBodyLimited(req, res, MqttPlus.MAX_BODY_BYTES);
                        if (body === null) return; // Fehlerantwort wurde bereits gesendet
                        try {
                            const data = JSON.parse(body);
                            if (data.template) {
                                await this.setStateAsync("config.syncTemplate", data.template, true);
                                this.log.info("New remote sync template saved.");
                                res.writeHead(200, { "Content-Type": "application/json" });
                                res.end(JSON.stringify({ success: true }));
                            } else {
                                throw new Error("No template received");
                            }
                        } catch (e: any) {
                            res.writeHead(500, { "Content-Type": "application/json" });
                            res.end(JSON.stringify({ success: false, error: e.message }));
                        }
                    }
                    else if (pathname === "/api/upload-backup" && req.method === "POST") {
                        const body = await this.readBodyLimited(req, res, MqttPlus.MAX_BODY_BYTES);
                        if (body === null) return; // Fehlerantwort wurde bereits gesendet
                        try {
                            const uploaded = JSON.parse(body);
                            if (uploaded && Array.isArray(uploaded.mappings)) {
                                this.log.info(`Restore started: ${uploaded.mappings.length} mappings found.`);
                                const adapterObj = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
                                if (adapterObj) {
                                    adapterObj.native.mappings = uploaded.mappings;
                                    if (uploaded.prefix) {
                                        adapterObj.native.targetBasePath = uploaded.prefix;
                                        this.log.info(`Prefix set to ${uploaded.prefix}.`);
                                    }
                                    await this.setForeignObjectAsync(`system.adapter.${this.namespace}`, adapterObj);
                                    res.writeHead(200, { "Content-Type": "application/json" });
                                    res.end(JSON.stringify({ success: true, message: "Configuration restored. Adapter restarts..." }));
                                } else {
                                    throw new Error("Adapter object not found!");
                                }
                            } else {
                                throw new Error("Invalid file format: 'mappings' array missing.");
                            }
                        } catch (e: any) {
                            this.log.error(`Restore error: ${e.message}`);
                            res.writeHead(500, { "Content-Type": "application/json" });
                            res.end(JSON.stringify({ success: false, error: e.message }));
                        }
                    }
                    else {
                        res.writeHead(404);
                        res.end("Not found");
                    }
                } catch (e: any) {
                    // Ohne dieses catch würde ein Fehler in generateJsonTree() o.ä. nie eine
                    // Antwort senden - der Client hinge bis zum eigenen Timeout.
                    this.log.error(`[Dashboard] Unexpected error: ${e.message}`);
                    if (!res.headersSent) {
                        res.writeHead(500, { "Content-Type": "application/json" });
                        res.end(JSON.stringify({ success: false, error: "Internal error" }));
                    }
                }
            };

            // Optionales HTTPS: ohne Zertifikat/Key läuft der Server wie bisher über HTTP -
            // dann werden Basic-Auth-Zugangsdaten aber unverschlüsselt übertragen (siehe Warnung
            // in onReady). Mit beiden Feldern gesetzt wird verschlüsselt.
            let usesTls = false;
            if (this.config.dashboardTlsCert && this.config.dashboardTlsKey) {
                try {
                    const cert = this.normalizePem(this.config.dashboardTlsCert);
                    const key = this.normalizePem(this.config.dashboardTlsKey);
                    this.httpServer = https.createServer({ cert, key }, requestListener);
                    usesTls = true;
                } catch (e: any) {
                    this.log.error(`[Dashboard] TLS certificate/key invalid, falling back to HTTP: ${e.message}`);
                }
            }
            if (!this.httpServer) {
                this.httpServer = http.createServer(requestListener);
            }

            this.httpServer.on("connection", (socket) => {
                this.activeSockets.add(socket);
                socket.on("close", () => this.activeSockets.delete(socket));
            });

            const bindHost = this.config.bind || "0.0.0.0";
            // Bei einem Update/Neustart hält der alte Prozess den Port oft noch einige Sekunden.
            // Deshalb erst mehrfach neu versuchen, statt sofort (und dauerhaft) aufzugeben.
            let listenAttempts = 0;
            const tryListen = () => {
                listenAttempts++;
                this.httpServer!.listen(port, bindHost);
            };
            this.httpServer.on("listening", () => {
                this.log.info(`Dashboard web server running on ${usesTls ? "https" : "http"}://${bindHost}:${port}`);
                this.setState("info.connection", true, true);
            });
            this.httpServer.on("error", (e: any) => {
                if (e.code === "EADDRINUSE" && listenAttempts < MqttPlus.LISTEN_ATTEMPTS && !this.unloaded) {
                    this.log.warn(`Port ${port} is still in use - retry ${listenAttempts + 1}/${MqttPlus.LISTEN_ATTEMPTS} in ${MqttPlus.LISTEN_RETRY_MS / 1000} s.`);
                    this.setTimeout(tryListen, MqttPlus.LISTEN_RETRY_MS);
                    return;
                }
                this.log.error(`Web server error: ${e.message}`);
                this.setState("info.connection", false, true);
                if (e.code === "EADDRINUSE") {
                    this.log.error(`Port ${port} is permanently in use - stopping the adapter so it does not keep running "green" without dashboard.`);
                    // terminate() statt eines harten Prozess-Endes: beendet im Compact Mode nur
                    // diese Instanz, nicht den gesamten Host-Prozess.
                    this.terminate("EADDRINUSE", utils.EXIT_CODES.ADAPTER_REQUESTED_TERMINATION);
                }
            });
            tryListen();
        } catch (e: any) {
            this.log.error(`Could not start web server: ${e.message}`);
            this.setState("info.connection", false, true);
        }
    }

    private getDashboardHtml(): string {
        return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MQTT Plus Dashboard</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f0f2f5; color: #333; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #0078d4; border-bottom: 2px solid #0078d4; padding-bottom: 10px; }
        h2 { margin-top: 30px; color: #444; }
        .status-bar { display: flex; gap: 20px; margin-bottom: 20px; background: #e6f0ff; padding: 15px; border-radius: 4px; }
        .status-item { font-weight: bold; }
        .status-value { color: #0078d4; }
        textarea { width: 100%; height: 150px; font-family: monospace; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        pre { background: #282c34; color: #abb2bf; padding: 15px; border-radius: 4px; overflow: auto; max-height: 500px; }
        button { background: #0078d4; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-size: 14px; margin-right: 10px; }
        button:hover { background: #005a9e; }
        button.secondary { background: #6c757d; }
        button.secondary:hover { background: #5a6268; }
        .help { font-size: 0.9em; color: #666; margin-top: 5px; }
        .upload-area { border: 2px dashed #ccc; padding: 20px; text-align: center; margin-bottom: 20px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>MQTT Plus Dashboard</h1>

        <div class="status-bar">
            <div class="status-item">Watchdog: <span id="wd" class="status-value">Loading...</span></div>
            <div class="status-item">Mappings: <span id="map" class="status-value">0</span></div>
            <div class="status-item">Uptime: <span id="up" class="status-value">0s</span></div>
        </div>

        <h2>Backup & Restore</h2>
        <div class="upload-area">
            <button onclick="downloadJson()">Download Backup (.json)</button>
            <span style="margin: 0 15px;">|</span>
            <input type="file" id="restoreFile" accept=".json" />
            <button class="secondary" onclick="uploadBackup()">Restore backup</button>
        </div>

        <h2>JSON structure preview</h2>
        <div style="margin-bottom: 10px;">
            <button class="secondary" onclick="loadJson()">Refresh preview</button>
        </div>
        <pre id="jsonViewer">Loading data...</pre>

        <h2>Remote sync configuration</h2>
        <p>Define what the JSON for the remote sync (POST request) looks like.</p>
        <div class="help">Placeholders: %ID%, %MQTT%, %VAL%, %TS% (last update), %LC% (last change), %ACK%, %Q% (quality), %UNIT%, %PREFIX%, %DIR%</div>
        <textarea id="templateEditor"></textarea>
        <div style="margin-top: 10px;">
            <button onclick="saveTemplate()">Save template</button>
            <button class="secondary" onclick="resetTemplate()">Restore default</button>
        </div>
    </div>

    <script>
        const defaultTemplate = '{"id": "%ID%", "topic": "%MQTT%", "value": %VAL%, "ts": %TS%, "unit": "%UNIT%", "prefix": "%PREFIX%", "dir": "%DIR%"}';

        async function loadStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('wd').innerText = data.watchdog;
                document.getElementById('map').innerText = data.mappings;
                document.getElementById('up').innerText = Math.round(data.uptime) + 's';
                if (!document.getElementById('templateEditor').value) {
                    document.getElementById('templateEditor').value = data.syncTemplate || defaultTemplate;
                }
            } catch(e) { console.error(e); }
        }

        async function loadJson() {
            try {
                document.getElementById('jsonViewer').innerText = "Loading...";
                const res = await fetch('/api/json');
                const data = await res.json();
                document.getElementById('jsonViewer').innerText = JSON.stringify(data.structure, null, 4);
                window.lastJson = data;
            } catch(e) {
                document.getElementById('jsonViewer').innerText = "Error while loading: " + e;
            }
        }

        function downloadJson() {
            if(!window.lastJson) {
                fetch('/api/json').then(r => r.json()).then(data => {
                    window.lastJson = data;
                    executeDownload();
                });
            } else {
                executeDownload();
            }
        }

        function executeDownload() {
            const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(window.lastJson, null, 4));
            const downloadAnchorNode = document.createElement('a');
            downloadAnchorNode.setAttribute("href",     dataStr);
            downloadAnchorNode.setAttribute("download", "mqtt_plus_backup.json");
            document.body.appendChild(downloadAnchorNode);
            downloadAnchorNode.click();
            downloadAnchorNode.remove();
        }

        async function uploadBackup() {
            const fileInput = document.getElementById('restoreFile');
            if(fileInput.files.length === 0) return alert("Please select a file first!");

            const file = fileInput.files[0];
            const reader = new FileReader();

            reader.onload = async function(e) {
                try {
                    const jsonContent = e.target.result;
                    const parsed = JSON.parse(jsonContent);
                    if(!parsed.mappings) throw new Error("No mappings found in file!");

                    if(!confirm("WARNING: This overwrites the current configuration and restarts the adapter. Continue?")) return;

                    const res = await fetch('/api/upload-backup', {
                        method: 'POST',
                        body: jsonContent
                    });
                    const ret = await res.json();

                    if(ret.success) {
                        alert(ret.message);
                        location.reload();
                    } else {
                        alert("Restore error: " + ret.error);
                    }
                } catch(err) {
                    alert("File error: " + err);
                }
            };
            reader.readAsText(file);
        }

        async function saveTemplate() {
            const tpl = document.getElementById('templateEditor').value;
            try {
                const res = await fetch('/api/save-template', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({template: tpl})
                });
                const ret = await res.json();
                if(ret.success) alert("Saved!");
                else alert("Error: " + ret.error);
            } catch(e) { alert("Send error: " + e); }
        }

        function resetTemplate() {
            document.getElementById('templateEditor').value = defaultTemplate;
        }

        loadStatus();
        loadJson();
        window.setInterval(loadStatus, 5000);
    </script>
</body>
</html>
        `;
    }

    private getDefaultSyncTemplate(): string {
        return '{"id": "%ID%", "topic": "%MQTT%", "value": %VAL%, "ts": %TS%, "unit": "%UNIT%", "prefix": "%PREFIX%", "dir": "%DIR%"}';
    }

    // Schreibt den Watchdog-Status nur bei Änderung oder maximal alle 30s - verhindert
    // hunderte States-DB-Schreibvorgänge pro Minute bei aktivem Datenverkehr (z.B. auf SD-Karte).
    // Ausnahme: info.lastCycle bei isCycleEnd=true (echter Zyklusabschluss) wird immer sofort
    // geschrieben, sonst könnte der Zeitstempel bei kurzen Intervallen ohne Statuswechsel
    // zwischen zwei Zyklen bis zu 30s hinter dem tatsächlichen letzten Zyklus zurückbleiben.
    private updateWatchdog(status: string, isCycleEnd: boolean = false): void {
        const now = Date.now();

        if (isCycleEnd) {
            this.setState("info.lastCycle", now, true);
        }

        const changed = status !== this.lastWatchdogStatus;
        if (!changed && now - this.lastWatchdogWriteTs < 30000) return;

        this.lastWatchdogStatus = status;
        this.lastWatchdogWriteTs = now;
        this.currentWatchdogStatus = `${status} (${new Date().toLocaleTimeString()})`;
        this.setState("watchdog", this.currentWatchdogStatus, true);
        // Maschinenlesbare Variante zusätzlich zum lokalisierten Text-State: reiner Status-String
        // plus Unix-Timestamp statt serverlokalem toLocaleTimeString().
        this.setState("info.status", status, true);
        if (!isCycleEnd) {
            this.setState("info.lastCycle", now, true);
        }
    }

    private async onUnload(callback: () => void): Promise<void> {
        this.unloaded = true;
        try {
            [this.updateInterval, this.forceSyncInterval, this.syncInterval].forEach(i => {
                if (i) this.clearInterval(i);
            });

            if (this.httpServer) {
                for (const socket of this.activeSockets) socket.destroy();
                this.activeSockets.clear();
                await new Promise<void>(resolve => this.httpServer!.close(() => resolve()));
            }

            try {
                const allIds = new Set<string>([...this.sourceToMappings.keys(), ...this.targetToMappings.keys()]);
                if (allIds.size > 0) {
                    await this.unsubscribeForeignStatesAsync([...allIds]);
                }
            } catch (e: any) {
                this.log.debug(`Unsubscribe error: ${e.message}`);
            }

            await this.setStateAsync("info.connection", false, true);
            this.log.info("cleaned everything up...");
            callback();
        } catch (e: any) {
            this.log.error(`Error during shutdown: ${e.message}`);
            callback();
        }
    }
}

if (require.main !== module) {
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new MqttPlus(options);
} else {
    (() => new MqttPlus())();
}

interface MappingEntry {
    id: string;
    mqttName: string;
    dir: "in" | "out" | "both";
    type: "auto" | "round" | "boolToNum" | "numToBool";
    unit?: string;
    ackFilter?: "confirmed" | "any";
    decimals?: number;
    topicMode?: "single" | "dual";
    commandSuffix?: string;
    staleAfterMin?: number | string | null;
    syncMode?: "standard" | "refresh" | "force";
    fullTargetPath?: string;
    commandPath?: string;
}
declare global {
    namespace ioBroker {
        interface AdapterConfig {
            targetBasePath: string;
            updateIntervalSec: number;
            logTransfers: boolean;
            mappings: MappingEntry[];
            syncUrl: string;
            syncIntervalMin: number;
            port: number;
            bind: string;
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
export {};

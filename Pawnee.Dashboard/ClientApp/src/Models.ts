import { StatusEvent } from "./Protocol";

export interface Activity {
    finished: number;
    instanceCount: number;
    instances: {
        [instance: number]: StatusEvent;
    };
    errors: StatusEvent[];
}

export interface Stage {
    name: string;
    inputs: Stage[];
    outputs: Stage[];
    positioned?: boolean;
    jobsWaiting: number;
    jobsRunning: number;
    activities: {
        [name: string]: Activity
    };
}

export interface Log {
    [seqNo: number]: StatusEvent;
    maxSeqNo?: number;
    minSeqNo?: number;
}
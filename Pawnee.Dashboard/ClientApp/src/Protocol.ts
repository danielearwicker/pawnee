export interface StageDescriptor {
  Name: string;
  Outputs: string[];
}

export interface StatusEvent {
  stage: string;
  aspect: string;
  instance?: number;
  instanceCount?: number;
  sample: string;
  count: number;
  perSecond: number;
  message: string;
  status: 0 | 1 | 2;
  timeStamp: string;
  sequenceNumber: number;
  virtualBytes: number;
  workingSet: number;
}

export interface QueueItem {
  stage: string;
  claimed: Date | null;
  content: string;
  worker?: string;
}

export interface QueueState {
  version: number;
  items: QueueItem[];
}

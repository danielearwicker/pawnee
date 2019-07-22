/* eslint-disable no-loop-func */
import * as signalR from "@aspnet/signalr";
import { QueueItem, StatusEvent } from "./Protocol";

export interface SignalHandler {
    connected(): void;
    disconnected(): void;
    queueUpdated(version: number, items: QueueItem[]): void;
    progressLogged(update: StatusEvent): void;
}

export class Connection {

    private handlers: SignalHandler[] = [];

    public isSignalRConnected = false;

    constructor() {
        this.connectToSignalR();
    }

    addHandler(handler: SignalHandler) {
        this.handlers.push(handler);
    }

    removeHandler(handler: SignalHandler) {
        const i = this.handlers.indexOf(handler);
        if (i !== -1) this.handlers.splice(i, 1);
    }

    async connectToSignalR() {

        const connection = new signalR.HubConnectionBuilder()
            .withUrl("/pawnee")
            .configureLogging(signalR.LogLevel.Information)
            .build();

        connection.onclose(async () => {
            this.isSignalRConnected = false;
            this.handlers.forEach(h => h.disconnected());

            setTimeout(() => this.connectToSignalR(), 2000);
        });

        connection.on("QueueUpdated", (version, items) =>
            this.handlers.forEach(h => h.queueUpdated(version, items)));

        connection.on("ProgressLogged", update =>
            this.handlers.forEach(h => h.progressLogged(update)));

        await connection.start();

        this.isSignalRConnected = true;
        this.handlers.forEach(h => h.connected());
    }
}


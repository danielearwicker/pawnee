/* eslint-disable no-loop-func */
import React, { Component } from 'react';
import { Connection, SignalHandler } from "./Connection";
import { BrowserRouter, Route, NavLink, Redirect, Switch } from "react-router-dom";
import { QueueItem, StatusEvent, StageDescriptor, QueueState } from "./Protocol";
import { LogPage } from "./LogPage";
import { Workers } from "./Workers";
import { Stage, Log } from "./Models";
import "./Home.css";
import { Pipeline } from './Pipeline';
import { PipelineStage } from './PipelineStage';

const connection = new Connection();

interface HomeState {
    layers: Stage[][];
    stages: {
        [name: string]: Stage;
    };
    queue: QueueState;
    log: Log;
}

export class Home extends Component<{}, HomeState> implements SignalHandler {

    constructor(props: {}) {
        super(props);

        this.state = {
            layers: [],
            stages: {},
            log: {},
            queue: {
                version: -1,
                items: []
            }
        };
    }

    componentDidMount() {
        connection.addHandler(this);

        if (connection.isSignalRConnected) {
            this.loadPipeline();
        }
    }

    connected() {
        this.loadPipeline();
    }

    disconnected() { }

    componentWillUnmount() {
        connection.removeHandler(this);
    }

    queueUpdated(version: number, items: QueueItem[]) {
        if (version <= this.state.queue.version) return;

        const stages = { ...this.state.stages };

        for (const stage of Object.values(stages)) {
            stage.jobsWaiting = stage.jobsRunning = 0;
        }

        for (const item of items) {
            var stage = stages[item.stage];
            if (stage) {
                if (item.claimed) {
                    stage.jobsRunning++;
                } else {
                    stage.jobsWaiting++;
                }
            }
        }

        this.setState({ stages, queue: { version, items } });
    }

    updateStageLog(stages: { [name: string]: Stage }, log: Log, update: StatusEvent) {
        log[update.sequenceNumber] = update;
        log.maxSeqNo = Math.max(log.maxSeqNo || 0, update.sequenceNumber);
        log.minSeqNo = Math.min(log.minSeqNo || 0, update.sequenceNumber);

        const stage = stages[update.stage];
        if (!stage) return;

        if (update.message && update.message.indexOf("Operations that change non-concurrent") !== -1) {
            console.log(update);
        }

        const aspectName = update.aspect || "(general)";

        const aspect = stage.activities[aspectName] ||
            (stage.activities[aspectName] = {
                instances: {},
                errors: [],
                finished: 0,
                instanceCount: 0
            });

        if (update.instanceCount) {
            aspect.instanceCount = update.instanceCount;
        }

        if (update.status === 2) {
            aspect.errors.push(update);
        }

        if (update.status === 1) {
            aspect.finished++;
        }

        var oldUpdate = aspect.instances[update.instance || 0];
        if (!oldUpdate || update.timeStamp > oldUpdate.timeStamp) {
            aspect.instances[update.instance || 0] = update;
        }
    }

    progressLogged(update: StatusEvent) {
        const stages = { ...this.state.stages };

        this.updateStageLog(stages, this.state.log, update);

        this.setState({ stages, log: this.state.log });
    }

    async fetchJson<T>(uri: string) {
        const req = await fetch(`/api/pawnee/${uri}`);
        return await req.json() as T;
    }

    async loadPipeline() {
        const queue = await this.fetchJson<QueueState>("queue");
        this.queueUpdated(queue.version, queue.items);

        const descriptions = await this.fetchJson<StageDescriptor[]>("pipeline");

        let stages: { [name: string]: Stage } = {};

        const getStage = (name: string) =>
            stages[name] || (stages[name] = {
                name,
                inputs: [],
                outputs: [],
                positioned: false,
                jobsWaiting: 0,
                jobsRunning: 0,
                activities: {}
            });

        for (const descr of descriptions) {
            const stage = getStage(descr.Name);
            for (const outputName of descr.Outputs) {
                const outputStage = getStage(outputName);
                outputStage.inputs.push(stage);
                stage.outputs.push(outputStage);
            }
        }

        const layers: Stage[][] = [];

        let remaining = Object.values(stages);

        while (remaining.length) {

            const layer = remaining.filter(u => u.inputs.every(i => !!i.positioned));
            layers.push(layer);

            for (const member of layer) {
                member.positioned = true;
                remaining = remaining.filter(r => layer.indexOf(r) === -1);
            }
        }

        this.setState({ layers, stages });

        const batchSize = 1000;
        const overlap = 50;
        const batchCount = 50;
        for (var skip = batchCount * batchSize;
            skip >= 0;
            skip -= batchSize) {

            const log = await this.fetchJson<StatusEvent[]>(`log?skip=${skip}&take=${batchSize + overlap}`);

            stages = { ...this.state.stages };

            for (const update of log) {
                this.updateStageLog(stages, this.state.log, update);
            }

            this.setState({ stages, log: this.state.log });
        }
    }

    render() {
        return (
            <BrowserRouter>
                <div className="dashboard">
                    <div className="pages">
                        <span className="logo">Pawnee</span>
                        <NavLink className="page-link" to={`/pipeline`}>Pipeline</NavLink>
                        <NavLink className="page-link" to={`/log/0`}>Log</NavLink>
                        <NavLink className="page-link" to={`/workers`}>Workers</NavLink>
                    </div>
                    <div className="page-content">
                        <Switch>
                            <Route path="/pipeline/:stage/:filter/:page" render={({match}) => (
                                <PipelineStage stage={match.params.stage} filter={match.params.filter} page={match.params.page} />
                            )}/>
                            <Route path="/pipeline" render={() => <Pipeline layers={this.state.layers} />} />
                            <Route path="/log/:page" render={({match}) => (
                                <LogPage log={this.state.log} page={parseInt(match.params.page, 10)}/>
                            )}/>
                            <Route path="/workers" render={() => <Workers queue={this.state.queue} />} />
                            <Route path="/" exact render={() => (
                                <Redirect to="/pipeline" />
                            )}/>
                        </Switch>
                    </div>
                </div>
            </BrowserRouter>
        );
    }
}

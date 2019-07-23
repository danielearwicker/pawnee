import React from 'react';
import { QueueState } from "./Protocol";
import { Link } from 'react-router-dom';
import "./Home.css";

export interface WorkersProps {
  queue: QueueState;
}

interface WorkerState {
    id: string;
    status: string;
}

export function Workers({ queue }: WorkersProps) {

    const workers: WorkerState[] = [];

    for (const item of queue.items) {
        if (item.stage[0] === '#') {
            const workerId = item.stage.substr(1);
            if (item.content !== "quit") {
                workers.push({
                    id: workerId,
                    status: item.content
                });
            }
        } else if (item.worker) {
            workers.push({
                id: item.worker,
                status: item.stage
            });
        }
    }

    return (
        <div className="workers">
            <table>
                <thead>
                    <tr>
                        <th>Id</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                {
                    workers.map(worker => (
                        <tr key={worker.id}>
                            <td>{worker.id}</td>
                            <td>{worker.status}</td>                                
                        </tr>
                    ))
                }
                </tbody>
            </table>      
        </div>
    );
}

import React from 'react';
import { Log } from "./Models";
import { PagingButtons } from "./PagingButtons";
import "./Home.css";

export interface LogPageProps {
  log: Log;
  page: number;
}

const scales = [
    "B",
    "KB",
    "MB",
    "GB",
    "TB"
];

function formatMemorySize(bytes: number) {

    let scale = 0;

    while (bytes > 1024) {
        scale++;
        bytes /= 1024;
    }

    return `${bytes.toFixed(2)} ${scales[scale]}`;
}

export function LogPage({ log, page }: LogPageProps) {

    const seqNoRange = (log.maxSeqNo || 0) - (log.minSeqNo || 0);

    const pageSize = 100;
    const pageCount = Math.ceil(seqNoRange / pageSize);

    const keys: number[] = [];

    for (let seqNo = (log.maxSeqNo || 0) - (page * pageSize);
         keys.length < pageSize && seqNo >= (log.minSeqNo || 0);
         seqNo--) {

        if (log[seqNo]) {
            keys.push(seqNo);
        }
    }

    return (
        <div className="grid">
            <div className="grid-table">
                <table>
                    <thead>
                        <tr>
                            <th>Stage</th>
                            <th>Aspect</th>
                            <th>Instance</th>
                            <th>Count</th>
                            <th>Per Second</th>
                            <th>Message</th>
                            <th>Status</th>
                            <th>TimeStamp</th>
                            <th>Sequence Number</th>
                            <th>Virtual Bytes</th>
                            <th>Working Set</th>
                        </tr>>
                    </thead>
                    <tbody>
                    {
                        keys.map(key => {
                            const entry = log[key];

                            const perSecond = entry.perSecond && entry.perSecond.toFixed(0);

                            const timeStamp = new Date(entry.timeStamp).toLocaleString();

                            return (
                                <tr key={key}>
                                    <td>{entry.stage}</td>
                                    <td>{entry.aspect}</td>
                                    <td>{entry.instance}/{entry.instanceCount}</td>
                                    <td>{entry.count}</td>
                                    <td>{perSecond}</td>
                                    <td>{entry.message}</td>
                                    <td>{entry.status}</td>
                                    <td>{timeStamp}</td>
                                    <td>{entry.sequenceNumber}</td>
                                    <td>{formatMemorySize(entry.virtualBytes)}</td>
                                    <td>{formatMemorySize(entry.workingSet)}</td>
                                </tr>
                            );
                        })
                    }
                    </tbody>
                </table>
            </div>
            <PagingButtons prefix="log" page={page} pageCount={pageCount} />
        </div>
    );
}

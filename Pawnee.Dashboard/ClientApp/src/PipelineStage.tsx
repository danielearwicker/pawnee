import React, { useState, useEffect } from "react";
import { PagingButtons } from "./PagingButtons";
import "./Home.css";

export interface PipelineStageProps {
    stage: string;
    page: number;
    filter: string;
}

interface Tabulation {
    columns: string[];
    rows: string[][];
}

export function PipelineStage({ stage, page, filter }: PipelineStageProps) {

    const [inputFilter, setInputFilter] = useState<string>(filter);

    const [tabulation, setTabulation] = useState<Tabulation>({
        columns: [],
        rows: []
    });

    useEffect(() => {
        const size = 100;

        const fixedFilter = filter === "_" ? "" : filter;

        fetch(`/api/pawnee/pipeline/${stage}?skip=${page * size}&take=${size}&filter=${fixedFilter}`)
            .then(r => r.json())
            .then(setTabulation);

    }, [stage, page]);

    return (
        <div className="grid">
            <div className="grid-table">
                <table>
                    <thead>
                        { tabulation.columns.map(column => <th>{column}</th>) }
                    </thead>
                    <tbody>
                    {
                        tabulation.rows.map(row => (
                            <tr>
                                { row.map(cell => <td>{cell}</td>) }
                            </tr>
                        ))
                    }
                    </tbody>
                </table>
            </div>
            <div className="grid-footer">
                <div className="grid-filter">
                    <input value={inputFilter} onChange={e => setInputFilter(e.target.value)}></input>
                    <a href={`/pipeline/${stage}/${inputFilter}/0`}>Apply Filter</a>
                </div>
                <PagingButtons prefix={`pipeline/${stage}/${filter}`} page={page} />
            </div>
            
        </div>
    );
}

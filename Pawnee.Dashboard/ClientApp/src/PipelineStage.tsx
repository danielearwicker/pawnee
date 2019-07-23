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

interface Expansion {
    [id: string]: boolean;
}

export function PipelineStage({ stage, page, filter }: PipelineStageProps) {

    const [inputFilter, setInputFilter] = useState<string>(filter);

    const [tabulation, setTabulation] = useState<Tabulation>({
        columns: [],
        rows: []
    });

    const [expansion, setExpansion] = useState<Expansion>({});

    function toggleExpansion(id: string) {
        setExpansion({ ...expansion, [id]: !expansion[id] })
    }

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
                        <tr>
                            <th></th>
                            { tabulation.columns.map(column => <th>{column}</th>) }
                        </tr>
                    </thead>
                    <tbody>
                    {
                        tabulation.rows.map(row => (
                            <>
                            <tr>
                                <th onClick={() => toggleExpansion(row[1])}>
                                {
                                    expansion[row[1]] ? "^" : "+"
                                }
                                </th>
                                { row.map(cell => <td>{cell}</td>) }
                            </tr>
                            {
                                expansion[row[1]] ? (
                                    <tr>
                                        <td colSpan={row.length + 1}>
                                            <pre>
                                            {
                                                JSON.stringify(row, null, 4)
                                            }
                                            </pre>
                                        </td>
                                    </tr>
                                ) : undefined  
                            }
                            </>
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

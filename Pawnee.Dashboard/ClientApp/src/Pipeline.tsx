import React from 'react';
import { Stage } from "./Models";
import "./Home.css";
import { StageNode } from './StageNode';

export interface PipelineProps {
  layers: Stage[][];
}

export function Pipeline({ layers }: PipelineProps) {
    return (
        <div className="pipeline">
        {
            layers.map(layer => (
                <div className="layer">
                    { layer.map(stage => <StageNode key={stage.name} stage={stage}/>) }
                </div>
            ))
        }
        </div>
    );
}

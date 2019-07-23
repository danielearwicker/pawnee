import React from 'react';
import { Stage } from "./Models";
import "./Home.css";
import { ActivitySection } from './ActivitySection';
import { Link } from 'react-router-dom';

export interface StageNodeProps {
  stage: Stage;
}

export function StageNode({ stage }: StageNodeProps) {

    const sortedActivities = Object.keys(stage.activities);
    sortedActivities.sort();

    return (
        <div className="stage">
            <div className="title">
                <Link to={`/pipeline/${stage.name}/_/0`}>{stage.name}</Link>            
                {
                    stage.jobsRunning > 0 ? (
                    <span className="running">{stage.jobsRunning}</span>
                    ) : undefined
                }
                {
                    stage.jobsWaiting > 0 ? (
                    <span className="waiting">{stage.jobsWaiting}</span>
                    ) : undefined
                }
            </div>
            <ul className="activities">
            {
                sortedActivities.map(activityKey => (
                    <ActivitySection key={activityKey} activityKey={activityKey} activity={stage.activities[activityKey]} />
                ))                    
            }
            </ul>
        </div>
    );
}

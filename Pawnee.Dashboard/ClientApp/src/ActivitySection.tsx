import React from 'react';
import { Activity } from "./Models";
import "./Home.css";

export interface ActivitySectionProps {
    activityKey: string;
    activity: Activity;
}

export function ActivitySection({ activityKey, activity }: ActivitySectionProps) {

    return (
        <li>
            <span className="activity-key">{activityKey}</span>
            {
                activity.errors.length ? (
                    activity.errors.map(e => (
                        <p className="error">{e.message}</p>
                    ))
                ) : undefined
            }
            <ul>
                <li>
                    Finished {activity.finished} of {activity.instanceCount}
                </li>
                {
                    Object.keys(activity.instances)
                        .map(key => ({ key, instance: activity.instances[parseInt(key, 10)] }))
                        .filter(({ instance }) => instance.status === 0)
                        .map(({ key, instance }) => (
                            <li>
                                <span className="instance-key">{key}</span>
                                {
                                    instance.message ? <span className="instance-message">{instance.message}</span> : undefined
                                }
                                {
                                    instance.count ? <span className="instance-count">{instance.count} 
                                                        ({instance.perSecond.toFixed(0)}/sec)</span> : undefined
                                }
                            </li>
                        ))
                }
            </ul>
        </li>
    );
}

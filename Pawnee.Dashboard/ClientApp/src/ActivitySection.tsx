import React from 'react';
import { Activity } from "./Models";
import "./Home.css";

export interface ActivitySectionProps {
    activityKey: string;
    activity: Activity;
}

export function ActivitySection({ activityKey, activity }: ActivitySectionProps) {

    const errorTip = activity.errors.map(e => e.message).join("\n");

    return (
        <li>
            <span className="activity-key">{activityKey}</span>
            <ul>
                {
                    activity.errors.length ? (
                        <li title={errorTip}>{activity.errors.length} errors occurred, causing retries.</li>
                    ) : undefined
                }
                <li>
                    Finished {activity.finished} of {activity.instanceCount}
                </li>
                {
                    Object.keys(activity.instances)
                        .map(key => ({ key, instance: activity.instances[parseInt(key, 10)] }))
                        .filter(({ instance }) => instance.status === 0)
                        .map(({ key, instance }) => (
                            <li key={key}>
                                <span className="instance-key">{key}</span>
                                {
                                    instance.message ? <span className="instance-message">{instance.message}</span> : undefined
                                }
                                {
                                    instance.count ? (
                                        <span className="instance-count">
                                            {instance.count} ({instance.perSecond.toFixed(0)}/sec)
                                        </span>
                                    ) : undefined
                                }
                            </li>
                        ))
                }
            </ul>
        </li>
    );
}

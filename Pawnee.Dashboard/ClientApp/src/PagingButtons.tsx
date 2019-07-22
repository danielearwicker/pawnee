import React from 'react';
import { Link } from 'react-router-dom';
import "./Home.css";

interface PagingButtonProps {
    enabled: boolean;
    label: string;
    page: number;
    prefix: string;
}

export function PagingButton({enabled, label, page, prefix}: PagingButtonProps) {
    return enabled ? (
        <div className="paging-button">
            <Link to={`/${prefix}/${page}`}>{label}</Link>
        </div>
    ) : (
        <div className="paging-button disabled">
            {label}
        </div>
    );
}

interface PagingButtonsProps {
    page: number | string;
    pageCount?: number;
    prefix: string;
}

export function PagingButtons({page, pageCount, prefix}: PagingButtonsProps) {

    page = typeof page === "string" ? parseInt(page) : page;

    return (
        <div className="paging">
            <PagingButton label="First Page" enabled={page > 0} page={0} prefix={prefix} />
            <PagingButton label="<" enabled={page > 0} page={page - 1} prefix={prefix} />
            <PagingButton label=">" 
                          enabled={pageCount === undefined || page < pageCount - 1} 
                          page={page + 1} 
                          prefix={prefix} />
            <PagingButton label="Last Page"
                          enabled={pageCount !== undefined && page < pageCount - 1} 
                          page={(pageCount || 1) - 1} 
                          prefix={prefix} />
        </div>
    );
}

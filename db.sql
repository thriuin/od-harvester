
-- Scanned Geogratis records

CREATE TABLE geogratis_records (
    id serial PRIMARY KEY NOT NULL,
    uuid TEXT,
    title_en TEXT,
    title_fr TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    updated TIMESTAMP WITHOUT TIME ZONE,
    edited TIMESTAMP WITHOUT TIME ZONE,
    state TEXT,
    json_record_en TEXT,
    json_record_fr TEXT,
    scanned TIMESTAMP WITHOUT TIME ZONE,
    od_status TEXT
);

-- Scanned EC ISO 19115 records

CREATE TABLE ec_records (
    id serial PRIMARY KEY NOT NULL,
    uuid TEXT,
    title TEXT,
    state TEXT,
    nap_record TEXT,
    scanned TIMESTAMP WITHOUT TIME ZONE
);

-- CKAN JSONL records

CREATE TABLE package_updates (
    id serial PRIMARY KEY NOT NULL,
    uuid TEXT,
    created TIMESTAMP WITHOUT TIME ZONE,
    updated TIMESTAMP WITHOUT TIME ZONE,
    ckan_json TEXT,
    message TEXT DEFAULT '',
    source TEXT
);

-- Application settings and run-time information

    CREATE TABLE settings (
        id serial PRIMARY KEY NOT NULL,
        setting_name TEXT NOT NULL,
        setting_value TEXT DEFAULT ''
    );




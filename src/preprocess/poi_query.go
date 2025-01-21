package preprocess

var PoiQuery = `
INSTALL spatial;
LOAD spatial;

COPY (
    WITH pois AS (
        SELECT
            id,
            names.primary AS name,
            geometry AS geom,
            'poi' AS class,
            NULL AS subclass
        FROM read_parquet('%DATADIR%place.geoparquet')
    ),
    relations AS (
        SELECT
            d.id,
            l.names.primary AS relation_name
        FROM pois d
        LEFT JOIN read_parquet('%DATADIR%division_area.geoparquet') l
        ON ST_Intersects(d.geom, l.geometry)
        WHERE
            (l.subtype = 'locality')
    ),
    aggregated_relations AS (
        SELECT
            d.id,
            d.name,
            ST_AsText(d.geom) AS geom,
            d.class,
            d.subclass,
            STRING_AGG(DISTINCT r.relation_name, ';') FILTER (WHERE r.relation_name IS NOT NULL) AS relation
        FROM pois d
        LEFT JOIN relations r
        ON d.id = r.id
        GROUP BY d.id, d.name, d.geom, d.class, d.subclass
    )
    SELECT
        id,
        name,
        geom,
        class,
        subclass,
        relation
    FROM aggregated_relations
) TO '%DATADIR%geocodeur_poi.geoparquet' (FORMAT 'PARQUET');
`

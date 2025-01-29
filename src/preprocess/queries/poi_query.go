package queries

var PoiQuery = `
INSTALL spatial;
LOAD spatial;

COPY (
    WITH clip AS (
        SELECT
            CASE
            WHEN '%COUNTRY%' != '' THEN (SELECT geometry from read_parquet('%DATADIR%division_area.geoparquet') where lower(names.primary) = '%COUNTRY%' and subtype = 'country')
            ELSE ST_GeomFromText('POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')
            END AS geom
    ),
    pois AS (
        SELECT
            a.id,
            a.names.primary AS name,
            a.geometry AS geom,
            'poi' AS class,
            NULL AS subclass
        FROM read_parquet('%DATADIR%place.geoparquet') AS a, clip AS b
        WHERE
            ST_Intersects(a.geometry, b.geom)
        AND
            a.confidence >= 0.4
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
) TO '%DATADIR%geocodeur_poi.parquet' (FORMAT 'PARQUET');
`

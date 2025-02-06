package queries

var DivisionQuery = `
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
    divisions AS (
        SELECT
            a.id,
            a.names.primary AS name,
            a.geometry AS geom,
            'division' AS class,
            a.subtype AS subclass
        FROM read_parquet('%DATADIR%division_area.geoparquet') AS a, clip AS b
        WHERE
            ST_Intersects(a.geometry, b.geom)
    ),
    relations AS (
        SELECT
            d.id,
            l.names.primary AS relation_name
        FROM divisions d
        LEFT JOIN read_parquet('%DATADIR%division_area.geoparquet') l
        ON ST_Contains(l.geometry, ST_Centroid(d.geom))
        WHERE
            (d.subclass = 'neighborhood' AND l.subtype = 'locality') OR
            (d.subclass = 'microhood' AND l.subtype = 'locality') OR
            (d.subclass = 'locality' AND l.subtype = 'county') OR
            (d.subclass = 'county' AND l.subtype = 'region')
    ),
    aggregated_relations AS (
        SELECT
            d.id,
            d.name,
            ST_AsGeoJSON(d.geom) AS geom,
            ST_AsText(ST_Centroid(d.geom)) AS centroid,
            d.class,
            d.subclass,
            STRING_AGG(DISTINCT r.relation_name, ';') FILTER (WHERE r.relation_name IS NOT NULL) AS relation
        FROM divisions d
        LEFT JOIN relations r
        ON d.id = r.id
        GROUP BY d.id, d.name, d.geom, d.class, d.subclass
    )
    SELECT
        id,
        name,
        geom,
        centroid,
        class,
        subclass,
        relation
    FROM aggregated_relations
) TO '%DATADIR%geocodeur_division.parquet' (FORMAT 'PARQUET');
`

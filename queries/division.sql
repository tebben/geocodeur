INSTALL spatial;
LOAD spatial;

COPY (
WITH divisions AS (
    SELECT
        id,
        names.primary AS name,
        geometry AS geom,
        'division' AS class,
        subtype AS subclass
    FROM read_parquet('./data/download/division_area.geoparquet')
),
relations AS (
    SELECT
        d.id,
        l.names.primary AS relation_name
    FROM divisions d
    LEFT JOIN read_parquet('./data/download/division_area.geoparquet') l
    ON ST_Intersects(d.geom, l.geometry)
    WHERE
        (d.subclass = 'neighborhood' AND l.subtype = 'locality') OR
        (d.subclass = 'locality' AND l.subtype = 'county')
),
aggregated_relations AS (
    SELECT
        d.id,
        d.name,
        ST_AsText(d.geom) AS geom,
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
    class,
    subclass,
    relation
FROM aggregated_relations
) TO './data/download/geocodeur_division.geoparquet' (FORMAT 'PARQUET');

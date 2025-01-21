INSTALL spatial;
LOAD spatial;

COPY (
WITH merged AS (
    SELECT
        ANY_VALUE(id) as id,
        names.primary as name,
        subtype AS class,
        class AS subclass,
        ST_LineMerge(ST_Union_Agg(geometry)) AS geom
    FROM
        read_parquet('./data/download/segment.geoparquet')
    WHERE
        subtype = 'road'
        AND names.primary IS NOT NULL
    GROUP BY
        names.primary, class, subtype
),
relations AS (
    SELECT
        m.id,
        l.names.primary AS relation_name
    FROM merged m
    LEFT JOIN read_parquet('./data/download/division_area.geoparquet') l
    ON ST_Intersects(m.geom, l.geometry)
    WHERE
        (m.subclass != 'motorway' AND l.subtype = 'locality') OR
        (m.subclass != 'motorway' AND l.subtype = 'county')
),
aggregated_relations AS (
    SELECT
        m.id,
        m.name,
        ST_AsText(m.geom) AS geom,
        m.class,
        m.subclass,
        STRING_AGG(DISTINCT r.relation_name, ';') FILTER (WHERE r.relation_name IS NOT NULL) AS relation
    FROM merged m
    LEFT JOIN relations r
    ON m.id = r.id
    GROUP BY m.id, m.name, m.geom, m.class, m.subclass
)
SELECT
    id,
    name,
    geom,
    class,
    subclass,
    relation
FROM aggregated_relations
) TO './data/download/geocodeur_segment.geoparquet' (FORMAT 'PARQUET');

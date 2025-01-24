package preprocess

var RoadQuery = `
INSTALL spatial;
LOAD spatial;


-- 1. Group all geometries with the same name and subclass
--    Buffer the geoms to create polygons and union the
--    polygons. polygons are melted together and represent
--    a road group.
-- 2. Create an envelope for each group for faster intersection
CREATE OR REPLACE TABLE road_groups AS (
    with polygons as (
        SELECT
            ANY_Value(id) as id,
            names.primary as name,
            subtype AS class,
            class AS subclass,
            ST_Union_Agg(ST_Buffer(geometry, 0.001)) AS geom
        FROM
            read_parquet('%DATADIR%segment.geoparquet')
        WHERE
            subtype = 'road'
        AND
            names.primary IS NOT NULL
        GROUP BY
            names.primary, class, subtype
    ),
    split as (
        SELECT
            REPLACE(UUID()::string, '-', '') AS id,
            name,
            class,
            subclass,
            ST_Envelope(unnest(ST_Dump(geom)).geom) AS geom
        FROM
            polygons
    )
    SELECT * from split
);

-- Create an index on the envelope for faster intersection
CREATE INDEX road_groups_geom_idx ON road_groups USING RTREE (geom);

-- 1. Select all road features with a name and subtype road
--    and find the road group they belong to.
-- 2. Merge all roads with the same name and subclass and group
--    them together.
-- 3. Find the locality or county the road is in.
-- 4. Aggregate the locality or county the road is in.
-- 5. Write the result to a new parquet file.
COPY (
    WITH features AS (
        SELECT
            a.id,
            a.names.primary as name,
            a.subtype AS class,
            a.class AS subclass,
            a.geometry AS geom,
            b.id as group_id
        FROM
            read_parquet('%DATADIR%segment.geoparquet') AS a
        LEFT JOIN
            road_groups AS b
        ON
            ST_Within(ST_Centroid(a.geometry), b.geom)
        AND
            a.class = b.subclass
        AND
            a.names.primary = b.name
        WHERE
            a.subtype = 'road'
        AND
            names.primary IS NOT NULL
        OR
            a.class = 'motorway'
    ),
    merged AS (
        SELECT
            REPLACE(UUID()::string, '-', '') AS id,
            a.name,
            a.class,
            a.subclass,
            ST_LineMerge(ST_Union_Agg(a.geom)) AS geom
        FROM
            features AS a
        GROUP BY
            a.name, a.class, a.subclass, a.group_id
    ),
    relations AS (
        SELECT
            a.id,
            b.names.primary AS relation_name
        FROM
            merged AS a
        LEFT JOIN
            read_parquet('%DATADIR%division_area.geoparquet') AS b
        ON
            ST_Intersects(a.geom, b.geometry)
        WHERE
            (a.subclass != 'motorway' AND b.subtype = 'locality') OR
            (a.subclass != 'motorway' AND b.subtype = 'county')
    ),
    aggregated_relations AS (
        SELECT
            a.id,
            a.name,
            ST_AsText(a.geom) AS geom,
            a.class,
            a.subclass,
            STRING_AGG(DISTINCT b.relation_name, ';') FILTER (WHERE b.relation_name IS NOT NULL) AS relation
        FROM
            merged AS a
        LEFT JOIN
            relations AS b
        ON
            a.id = b.id
        GROUP BY
            a.id, a.name, a.geom, a.class, a.subclass
    )
    SELECT
        id,
        name,
        geom,
        class,
        subclass,
        relation
    FROM aggregated_relations
) TO '%DATADIR%geocodeur_segment.geoparquet' (FORMAT 'PARQUET');
`

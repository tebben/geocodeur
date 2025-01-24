/*
Distinct class
┌───────────────┐
│     class     │
│    varchar    │
├───────────────┤
│ stream        │
│ drain         │
│ pond          │
│ lake          │
│ water         │
│ moat          │
│ fairway       │
│ ditch         │
│ canal         │
│ swimming_pool │
│ basin         │
│ river         │
├───────────────┤
│    12 rows    │
└───────────────┘

Distinct subtypes
┌────────────┐
│  subtype   │
│  varchar   │
├────────────┤
│ human_made │
│ river      │
│ reservoir  │
│ stream     │
│ pond       │
│ lake       │
│ canal      │
│ water      │
└────────────┘

Distinct geometry types
┌───────────────────────────┐
│ st_geometrytype(geometry) │
│       geometry_type       │
├───────────────────────────┤
│ POINT                     │
│ POLYGON                   │
│ MULTIPOLYGON              │
│ LINESTRING                │
└───────────────────────────┘

- Only water with names.primary
- subtype is most of the time the same as class, lets subtract the subtypes from it and just use class for
subclass of water, when there is a different subtype it's not very usefull anyway.
the swimming pool is human made
a ditch is a canal
fairway is a water
much wow!
- Features with lines are sometimes split up again and need to be merged/grouped
- Polygons are not directly split up but need to be grouped aswell when close and representing the same feature

ToDo:
- We have features 'duplicated' as lines and polygons, remove a line if it's within a polygon with the same name and subclass

Not sure:
There are points for swimming_pool, do we get from infrastructure aswell?
*/

package preprocess

var WaterQuery = `
INSTALL spatial;
LOAD spatial;


-- 1. Group all geometries with the same name and subclass
--    Buffer the geoms to create polygons and union the
--    polygons. polygons are melted together and represent
--    a road group.
-- 2. Create an envelope for each group for faster intersection
CREATE OR REPLACE TABLE water_groups AS (
    with polygons as (
        SELECT
            ANY_Value(id) as id,
            names.primary as name,
            'water' AS class,
            class AS subclass,
            ST_Union_Agg(ST_Buffer(geometry, 0.002)) AS geom
        FROM
            read_parquet('%DATADIR%water.geoparquet')
        WHERE
            names.primary IS NOT NULL
        GROUP BY
            names.primary, class
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
CREATE INDEX water_groups_geom_idx ON water_groups USING RTREE (geom);

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
            'water' AS class,
            a.class AS subclass,
            a.geometry AS geom,
            b.id as group_id
        FROM
            read_parquet('%DATADIR%water.geoparquet') AS a
        LEFT JOIN
            water_groups AS b
        ON
            ST_Within(ST_Centroid(a.geometry), b.geom)
        AND
            a.class = b.subclass
        AND
            a.names.primary = b.name
        WHERE
            names.primary IS NOT NULL
    ),
    merged_features AS (
        SELECT
            REPLACE(UUID()::string, '-', '') AS id,
            a.name,
            a.class,
            a.subclass,
            ST_Collect(ARRAY_AGG(a.geom)) AS geom
        FROM
            features AS a
        GROUP BY
            a.name, a.class, a.subclass, a.group_id
    ),
    relations AS (
        SELECT
            a.id,
            b.names.primary AS relation_name
        FROM merged_features AS a
        LEFT JOIN read_parquet('%DATADIR%division_area.geoparquet') b
        ON ST_Intersects(a.geom, b.geometry)
        WHERE
            a.subclass IN ('stream', 'drain', 'pond', 'lake', 'moat', 'fairway', 'ditch', 'swimming_pool', 'basin', 'water')
            AND b.subtype IN ('locality', 'county')
    ),
    aggregated_relations AS (
        SELECT
            a.id,
            a.name,
            ST_AsText(a.geom) AS geom,
            a.class,
            a.subclass,
            STRING_AGG(DISTINCT b.relation_name, ';') FILTER (WHERE b.relation_name IS NOT NULL) AS relation
        FROM merged_features a
        LEFT JOIN relations b
        ON a.id = b.id
        GROUP BY a.id, a.name, a.geom, a.class, a.subclass
    )
    SELECT
        id,
        name,
        geom,
        class,
        subclass,
        relation
    FROM aggregated_relations
) TO '%DATADIR%geocodeur_water.geoparquet' (FORMAT 'PARQUET');
`

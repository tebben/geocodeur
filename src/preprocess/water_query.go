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
- We have features 'duplicated' as lines and polygons, remove a line if it's within a polygon with the same name and subclass

Not sure:
There are points for swimming_pool, do we get from infrastructure aswell?

Processing:

In Overture Maps features can be cut up into separate lines and polygons, we need to find groups where name and class is the same
and are close to each other, then merge them into one feature. When sombody searches for a water body we don't want to return
50 results representing the same feature so we need to do some processing.

Using duckdb we do not have access to the ST_ClusterDBSCAN function and other helpful functions in PostGIS. To solve this we can
split this into a few steps:
- For every feature find neighbours that are close and have the same name and class
- Create a graph representation of the neighbours, this is a set number of iterations, this can be heavy and it's not 100%
    accurate, there is a possibility some features are still split up.
- Group the features by name, class and group id

ToDo
- Find a way to work around recursion
- Is this distance ok
- Do not make a collections if there is only single geometry type
- Remove lines if there is already a polygon representation for it polygon > polyline
*/

package preprocess

var WaterQuery = `
INSTALL spatial;
LOAD spatial;

-- Create a table with neighbours
DROP TABLE IF EXISTS neighbor_features;
CREATE TABLE neighbor_features AS (
    WITH features AS (
        SELECT
            id,
            names.primary AS name,
            'water' as class,
            class AS subclass,
            geometry as geom
        FROM
            read_parquet('%DATADIR%water.geoparquet')
        WHERE
            names.primary IS NOT NULL
    ),
  -- Identify adjacent features
  adjacent AS (
    SELECT
      l1.id as f1_id,
      l2.id as f2_id
    FROM
      features l1, features l2
    WHERE
        ST_DWithin(l1.geom, l2.geom, 0.005) -- 0.005 = around 550 meters
    AND
      l1.id != l2.id -- To avoid duplicate pairs
    AND
      l1.name = l2.name
    AND
      l1.subclass = l2.subclass
  ),
  -- Create a graph representation of adjacent lines
  neighbor_features AS (
    SELECT
      id,
      ARRAY_AGG(DISTINCT connected_id) as neighbors
    FROM (
      SELECT
        f1_id as id,
        f2_id as connected_id
      FROM
        adjacent
      UNION ALL
      SELECT
        f2_id as id,
        f1_id as connected_id
      FROM
        adjacent
    ) subquery
    GROUP BY
      id
  ) SELECT * FROM neighbor_features
);

-- Using recursion try to create groups of lines that are connected
-- The recursion depth is limited to 5 to avoid excessive computation
DROP TABLE IF EXISTS feature_groups;
CREATE TABLE feature_groups AS (
    WITH RECURSIVE construct AS (
        SELECT id, neighbors, 1 AS depth
        FROM neighbor_features
        UNION ALL
        SELECT a.id, ARRAY_AGG(DISTINCT x ORDER BY x) AS neighbors, a.depth + 1
        FROM (
            SELECT a.id, unnest(ARRAY_CONCAT(a.neighbors, b.neighbors)) AS x, a.depth
            FROM construct AS a
            JOIN neighbor_features AS b ON b.id = ANY(a.neighbors)
            WHERE a.id != b.id AND a.depth < 20  -- Limit the recursion depth
        ) AS a
        GROUP BY a.id, a.depth
    ),
    feature_groups as (
        SELECT DISTINCT neighbors as group
        FROM construct
        WHERE depth = (SELECT MAX(depth) FROM construct)
    )
    SELECT REPLACE(UUID()::string, '-', '') AS id, a.group from feature_groups a
);

-- Final step
COPY (
    WITH features AS (
        SELECT
            id,
            names.primary AS name,
            'water' AS class,
            class AS subclass,
            geometry AS geom
        FROM
            read_parquet('%DATADIR%water.geoparquet')
        WHERE
            names.primary IS NOT NULL
    ),
    featues_with_group AS (
        SELECT
            l.id,
            l.name,
            l.class,
            l.subclass,
            l.geom,
            (SELECT lg.id
            FROM feature_groups lg
            WHERE l.id = ANY(lg.group)
            LIMIT 1) AS group_id
        FROM
            features l
    ),
    merged_features AS (
        SELECT
            ANY_VALUE(a.id) AS id,
            a.name,
            a.class,
            a.subclass,
            ST_Collect(ARRAY_AGG(a.geom)) AS geom
        FROM
            featues_with_group a
        GROUP BY
            name, class, subclass, group_id
    ),
    relations AS (
        SELECT
            a.id,
            b.names.primary AS relation_name
        FROM merged_features AS a
        LEFT JOIN read_parquet('%DATADIR%division_area.geoparquet') b
        ON ST_Intersects(a.geom, b.geometry)
        WHERE
            a.subclass IN ('stream', 'drain', 'pond', 'lake', 'moat', 'fairwat', 'ditch', 'swimming_pool', 'basin', 'water')
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

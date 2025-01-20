load spatial;

-- Load your data from the Parquet files into DuckDB
CREATE TABLE aggregated_relations AS
WITH segments AS (
    SELECT
        id,
        names.primary AS name,
        geometry AS geom,
        subtype AS class,
        class AS subclass
    FROM read_parquet('segment.geoparquet')
    -- rail not realy usable, water is a line we will get waterbodies from a different dataset
    WHERE subtype IN ('road')
    AND names.primary IS NOT NULL
),
relations AS (
    SELECT
        s.id,
        l.names.primary AS relation_name
    FROM segments s
    LEFT JOIN read_parquet('division_area.geoparquet') l
    ON ST_Intersects(s.geom, l.geometry)
    WHERE
        (s.subclass != 'motorway' AND l.subtype = 'locality') OR
        (s.subclass != 'motorway' AND l.subtype = 'county')
),
agg AS (
    SELECT
        s.id,
        s.name,
        s.geom,
        s.class,
        s.subclass,
        ARRAY_AGG(r.relation_name) AS relation
    FROM segments s
    LEFT JOIN relations r
    ON s.id = r.id
    GROUP BY s.id, s.name, s.geom, s.class, s.subclass
)
SELECT * FROM agg;

-- Create a spatial index on the geometry column (using RTREE)
CREATE INDEX idx_geom_rtree ON aggregated_relations USING RTREE (geom);

CREATE TABLE road_graph (
    segment_id_1 INT,
    segment_id_2 INT,
    PRIMARY KEY (segment_id_1, segment_id_2)
);

-- Step 2: Create a recursive CTE to find all connected components
WITH RECURSIVE connected_components AS (
    -- Base case: Select each segment and start a new component
    SELECT
        id AS segment_id,
        id AS component_id,  -- Initial component_id is the segment's own ID
        ARRAY[id] AS connected_segments
    FROM aggregated_relations
    WHERE id IS NOT NULL

    UNION ALL

    -- Recursive case: Find connected segments (touching geometries)
    SELECT
        a2.id AS segment_id,
        cc.component_id,  -- Propagate the existing component_id
        ARRAY_APPEND(cc.connected_segments, a2.id) AS connected_segments
    FROM aggregated_relations a2
    JOIN connected_components cc
    ON ST_Touches(a2.geom, (SELECT geom FROM aggregated_relations WHERE id = cc.segment_id))  -- Check if the segment touches any of the existing connected components
    WHERE NOT a2.id = ANY(cc.connected_segments)  -- Avoid revisiting already connected segments
)
-- Step 3: Insert all connections into the road_graph table
INSERT INTO road_graph (segment_id_1, segment_id_2)
SELECT
    LEAST(segment_id, unnest(connected_segments)) AS segment_id_1,
    GREATEST(segment_id, unnest(connected_segments)) AS segment_id_2
FROM connected_components
WHERE array_length(connected_segments, 1) > 1;  -- Only consider components with more than 1 segment (i.e., connected segments)

-- Optional: Check the contents of the graph table
SELECT * FROM road_graph;

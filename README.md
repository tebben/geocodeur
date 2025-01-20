# geocodeur

## Temp

```sh
pip install onnxruntime-gpu
```

Create a flattened dataset containing only the data we need.

What is usefull information?
- id
- geometry
- primary name
- type (segment, poi, division, address, water)
- subtype (country, neighborhood, track, street, etc)

What is a parent in our context, let's say you want to search a street, how would you normally search this? I would search for the street name and the city name. So in the case of a street the parent would be the city. In the case of a city the parent would be the country. For highways there can be multiple parents but is not usefull anyway, you search for a highway like A2 you won't search by city/town.

The same goes for Points of Interest, the parent would be the city/town (locality).

## division-area

In geocodeur we want to find be able to find divisions, what subtypes benefit from having a parent when searching for a division?
region, neighborhood, locality, country

### Columns

```sh
┌─────────────┬────────────────────────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│ column_name │                                    column_type                                     │  null   │   key   │ default │  extra  │
│   varchar   │                                      varchar                                       │ varchar │ varchar │ varchar │ varchar │
├─────────────┼────────────────────────────────────────────────────────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ id          │ VARCHAR                                                                            │ YES     │         │         │         │
│ geometry    │ BLOB                                                                               │ YES     │         │         │         │
│ bbox        │ STRUCT(xmin FLOAT, xmax FLOAT, ymin FLOAT, ymax FLOAT)                             │ YES     │         │         │         │
│ country     │ VARCHAR                                                                            │ YES     │         │         │         │
│ version     │ INTEGER                                                                            │ YES     │         │         │         │
│ sources     │ STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, update_time VARCHAR…  │ YES     │         │         │         │
│ subtype     │ VARCHAR                                                                            │ YES     │         │         │         │
│ class       │ VARCHAR                                                                            │ YES     │         │         │         │
│ names       │ STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), rules STRUCT(variant VAR…  │ YES     │         │         │         │
│ region      │ VARCHAR                                                                            │ YES     │         │         │         │
│ division_id │ VARCHAR                                                                            │ YES     │         │         │         │
├─────────────┴────────────────────────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 11 rows                                                                                                                        6 columns │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

select distinct(subtype) from read_parquet('division_area.geoparquet');

┌──────────────┐
│   subtype    │
│   varchar    │
├──────────────┤
│ country      │
│ neighborhood │
│ locality     │
│ region       │
│ microhood    │
│ county       │
└──────────────┘

select distinct(class) from read_parquet('division_area.geoparquet');

┌─────────┐
│  class  │
│ varchar │
├─────────┤
│ land    │
└─────────┘
```


country = Netherlands (country)
region = Noord-Brabant (region)
neighborhood = Zuid, Pettelaarpark, De Moerputten (neighborhood) (buurt)
locality = 's-Hertogenbosch, Vught, Rosmalen (place)
microhood = Zuidoost, West, Wilhelminaplein (district) (wijk)
county = 's-Hertogenbosch, Vught, Boxtel (county/municipality)

For the netherlands microhood does not seem to make much sense at the point we can combine neighborhood and microhood.

## Parents

Parents, what combination is likely to be used when searching for a division?
- country -> None
- neighborhood -> locality
- locality -> county
- county -> None

## Query generation divisions

```sql
COPY (
WITH divisions AS (
    SELECT
        id,
        names.primary AS name,
        geometry AS geom,
        'division' AS class,
        subtype AS subclass
    FROM read_parquet('division_area.geoparquet')
),
relations AS (
    SELECT
        d.id,
        l.names.primary AS relation_name
    FROM divisions d
    LEFT JOIN read_parquet('division_area.geoparquet') l
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
) TO 'geocodeur_division.geoparquet' (FORMAT 'PARQUET');
```

## Segment

```sql
COPY (
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
    aggregated_relations AS (
        SELECT
            s.id,
            s.name,
            ST_AsText(s.geom) AS geom,
            s.class,
            s.subclass,
            STRING_AGG(DISTINCT r.relation_name, ';') FILTER (WHERE r.relation_name IS NOT NULL) AS relation
        FROM segments s
        LEFT JOIN relations r
        ON s.id = r.id
        GROUP BY s.id, s.name, s.geom, s.class, s.subclass
    )
    SELECT
        id,
        name,
        geom,
        class,
        subclass,
        relation
    FROM aggregated_relations
) TO 'geocodeur_segment.geoparquet' (FORMAT 'PARQUET');
```

```sql
-- overture can have multiple segments for a road we want to have the complete road

WITH RECURSIVE connected_segments(id, name, subclass, geom, relation) AS (
    -- Base case: Select the initial segments
    SELECT
        s.id,
        s.name,
        s.subclass,
        s.geom,
        s.relation
    FROM aggregated_relations s
    WHERE s.id IS NOT NULL

    UNION ALL

    -- Recursive case: Connect segments that touch each other
    SELECT
        ANY_VALUE(s2.id) AS id,  -- Use ANY_VALUE to avoid grouping by id
        cs.name,
        cs.subclass,
        ANY_VALUE(ST_Union(cs.geom, s2.geom)) AS geom,  -- Combine geometries using ST_Union
        STRING_AGG(DISTINCT s2.relation, ';') AS relation
    FROM connected_segments cs
    JOIN aggregated_relations s2
    ON ST_Touches(cs.geom, s2.geom) -- Ensure that they touch
    AND cs.name = s2.name
    AND cs.subclass = s2.subclass
    AND cs.id != s2.id -- Prevent self-joining
    GROUP BY cs.name, cs.subclass  -- Only group by name and subclass (no geom)
),
segments AS (
    SELECT
        id,
        names.primary AS name,
        geometry AS geom,
        subtype AS class,
        class AS subclass
    FROM read_parquet('segment.geoparquet')
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
aggregated_relations AS (
    SELECT
        s.id,
        s.name,
        s.geom AS geom,
        s.class,
        s.subclass,
        STRING_AGG(DISTINCT r.relation_name, ';') FILTER (WHERE r.relation_name IS NOT NULL) AS relation
    FROM segments s
    LEFT JOIN relations r
    ON s.id = r.id
    GROUP BY s.id, s.name, s.geom, s.class, s.subclass
),
final_segments AS (
    -- Final aggregation of all connected segments
    SELECT
        name,
        geom,  -- Merge the geometries of connected segments
        subclass,
        STRING_AGG(DISTINCT relation, ';') AS relation
    FROM connected_segments
    GROUP BY name, geom, subclass
)
-- Final output: Select the aggregated result
SELECT
    name,
    geom,
    subclass,
    relation
FROM final_segments
WHERE name = 'Van Veldekekade';
```

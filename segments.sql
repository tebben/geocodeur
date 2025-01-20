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
        s.geom,
        s.class,
        s.subclass,
        ARRAY_AGG(r.relation_name) AS relation
    FROM segments s
    LEFT JOIN relations r
    ON s.id = r.id
    GROUP BY s.id, s.name, s.geom, s.class, s.subclass
)
SELECT * FROM aggregated_relations where name = 'Van Veldekekade' and subclass = 'secondary';

--┌──────────────────────┬─────────────────┬──────────────────────────────────────────────────────────────────────────────────┬─────────┬───────────┬──────────────────────────────────────┐
--│          id          │      name       │                                       geom                                       │  class  │ subclass  │               relation               │
--│       varchar        │     varchar     │                                     geometry                                     │ varchar │  varchar  │              varchar[]               │
--├──────────────────────┼─────────────────┼──────────────────────────────────────────────────────────────────────────────────┼─────────┼───────────┼──────────────────────────────────────┤
--│ 0881fa4b6d7fffff04…  │ Van Veldekekade │ LINESTRING (5.3184028 51.6877243, 5.3179805 51.6877921, 5.3176684 51.6878483, …  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 08b1fa4b6d608fff04…  │ Van Veldekekade │ LINESTRING (5.3244618 51.6868137, 5.3242349 51.686847)                           │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 0881fa4b6d7fffff04…  │ Van Veldekekade │ LINESTRING (5.3244618 51.6868137, 5.3248969 51.6866893, 5.3249416 51.6866698, …  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 08b1fa4b6d6e5fff04…  │ Van Veldekekade │ LINESTRING (5.3251009 51.6865573, 5.3252066 51.6865688, 5.3252791 51.686579)     │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 08a1fa4b6d657fff04…  │ Van Veldekekade │ LINESTRING (5.3258811 51.6866192, 5.325739 51.6866634, 5.3256435 51.6866804, 5…  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 0881fa4b6d7fffff04…  │ Van Veldekekade │ LINESTRING (5.3242349 51.686847, 5.3230943 51.6870186, 5.3184028 51.6877243)     │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 0891fa4b6d67ffff04…  │ Van Veldekekade │ LINESTRING (5.3270499 51.6863772, 5.3270107 51.6864026, 5.3269608 51.6864307, …  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 08c1fa4b6d6565ff04…  │ Van Veldekekade │ LINESTRING (5.3252791 51.686579, 5.3252356 51.6866377)                           │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 0881fa4b6d7fffff04…  │ Van Veldekekade │ LINESTRING (5.3252356 51.6866377, 5.325175 51.6866022, 5.3251009 51.6865573)     │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 08a1fa4b6d657fff04…  │ Van Veldekekade │ LINESTRING (5.3252791 51.686579, 5.3253644 51.6866025, 5.3254415 51.6866205, 5…  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--│ 0881fa4b6d7fffff04…  │ Van Veldekekade │ LINESTRING (5.3252356 51.6866377, 5.3251988 51.6866689, 5.3251508 51.6866955, …  │ road    │ secondary │ ['s-Hertogenbosch, 's-Hertogenbosch] │
--├──────────────────────┴─────────────────┴──────────────────────────────────────────────────────────────────────────────────┴─────────┴───────────┴──────────────────────────────────────┤
--│ 11 rows                                                                                                                                                                      6 columns │
--└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

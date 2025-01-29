/*
describe select * from read_parquet('infrastructure.geoparquet');
┌─────────────┬────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│ column_name │                    column_type                     │  null   │   key   │ default │  extra  │
│   varchar   │                      varchar                       │ varchar │ varchar │ varchar │ varchar │
├─────────────┼────────────────────────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ id          │ VARCHAR                                            │ YES     │         │         │         │
│ geometry    │ GEOMETRY                                           │ YES     │         │         │         │
│ bbox        │ STRUCT(xmin FLOAT, xmax FLOAT, ymin FLOAT, ymax …  │ YES     │         │         │         │
│ version     │ INTEGER                                            │ YES     │         │         │         │
│ sources     │ STRUCT(property VARCHAR, dataset VARCHAR, record…  │ YES     │         │         │         │
│ subtype     │ VARCHAR                                            │ YES     │         │         │         │
│ class       │ VARCHAR                                            │ YES     │         │         │         │
│ surface     │ VARCHAR                                            │ YES     │         │         │         │
│ names       │ STRUCT("primary" VARCHAR, common MAP(VARCHAR, VA…  │ YES     │         │         │         │
│ level       │ INTEGER                                            │ YES     │         │         │         │
│ source_tags │ MAP(VARCHAR, VARCHAR)                              │ YES     │         │         │         │
│ wikidata    │ VARCHAR                                            │ YES     │         │         │         │
├─────────────┴────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 12 rows                                                                                        6 columns │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘

select distinct(ST_GeometryType(geometry)) from read_parquet('infrastructure.geoparquet');
┌───────────────────────────┐
│ st_geometrytype(geometry) │
│       geometry_type       │
├───────────────────────────┤
│ MULTIPOLYGON              │
│ POINT                     │
│ POLYGON                   │
│ LINESTRING                │
└───────────────────────────┘

select distinct(subtype) from read_parquet('infrastructure.geoparquet');
┌──────────────────┐
│     subtype      │
│     varchar      │
├──────────────────┤
│ power            │
│ transit          │
│ barrier          │
│ pedestrian       │
│ airport          │
│ utility          │
│ bridge           │
│ aerialway        │
│ communication    │
│ pier             │
│ tower            │
│ recreation       │
│ water            │
│ waste_management │
│ manhole          │
├──────────────────┤
│     15 rows      │
└──────────────────┘

select distinct(class) from read_parquet('infrastructure.geoparquet');

Gebruiken: [platform, parking_space, weir, defensive, water_tower, dam, drag_lift, fence, substation, radar, drinking_water, power_line, ]

not in: 'vending_machine', 'toilets', 'hedge', 'chain', 'border_control', 'atm', 'movable', 'sewer', 'pylon', 'waste_basket', 'guard_rail', 'city_wall', 'post_box', 'hose', 'handrail', 'minaret', 'bollard'

stop_position
platform
parking_space
toilets
power_pole
weir
defensive
water_tower
dam
t-bar
drag_lift
cable_barrier
fence
kerb
hedge
lighting
substation
radar
drinking_water
power_line
portal
cable
chain
international_airport
cattle_grid
hampshire_gate
drain
viaduct
catenary_mast
cable_car
border_control
monitoring
lightning_protection
parking
wall
bridge
full-height_turnstile
atm
swing_gate
height_restrictor
bridge_support
movable
kissing_gate
ditch
waste_disposal
sally_port
military_airport
switch
sewer
pylon
regional_airport
bench
bus_stop
waste_basket
cycle_barrier
retaining_wall
vending_machine
information
guard_rail
viewpoint
toll_booth
bus_station
silo
observation
city_wall
manhole
trestle
heliport
chair_lift
heliostat
jersey_barrier
block
airport_gate
power_tower
pipeline
plant
railway_halt
watchtower
camp_site
airport
cantilever
cutline
hose
post_box
recycling
taxiway
runway
boardwalk
bus_trap
entrance
cooling
bell_tower
ferry_terminal
covered
communication_line
aerialway_station
bicycle_parking
railway_station
barrier
generator
helipad
transformer
terminal
utility_pole
handrail
private_airport
siren
minaret
gate
bollard
lift_gate
mobile_phone_tower
communication_tower
minor_line
storage_tank
pier
insulator
stile
bump_gate
planter
connection
aqueduct
diving
gondola
*/

package queries

var InfraQuery = `
INSTALL spatial;
LOAD spatial;

CREATE OR REPLACE TABLE clipped_features AS (
 	WITH clip AS (
        SELECT
            CASE
            WHEN '%COUNTRY%' != '' THEN (SELECT geometry from read_parquet('%DATADIR%division_area.geoparquet') where lower(names.primary) = '%COUNTRY%' and subtype = 'country')
            ELSE ST_GeomFromText('POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')
            END AS geom
    )
    SELECT
		a.id,
		a.names.primary AS name,
		'infra' AS class,
		a.class AS subclass,
		a.geometry AS geom
	FROM
		read_parquet('%DATADIR%infrastructure.geoparquet') AS a, clip AS b
	WHERE
		names.primary IS NOT NULL
	AND
		a.class NOT IN ('vending_machine', 'toilets', 'hedge', 'chain', 'border_control', 'atm', 'movable', 'sewer', 'pylon', 'waste_basket', 'guard_rail', 'city_wall', 'post_box', 'hose', 'handrail', 'minaret', 'bollard')
	AND
		ST_Intersects(a.geometry, b.geom)
);

-- We can group together features that are close to each other with the same name, class and subclass
CREATE OR REPLACE TABLE feature_groups AS (
    with polygons as (
        SELECT
            ANY_Value(id) as id,
            name,
            class,
            subclass,
            ST_Union_Agg(ST_Buffer(geom, 0.001)) AS geom
        FROM
            clipped_features
        GROUP BY
            name, class, subclass
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
CREATE INDEX feature_groups_geom_idx ON feature_groups USING RTREE (geom);

COPY (
    WITH features AS (
        SELECT
            a.id,
            a.name,
            a.class,
            a.subclass,
            a.geom,
            b.id as group_id
        FROM
            clipped_features AS a
        LEFT JOIN
            feature_groups AS b
        ON
            ST_Within(ST_Centroid(a.geom), b.geom)
        AND
            a.subclass = b.subclass
        AND
            a.name = b.name
    ),
    merged AS (
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
        FROM
            merged AS a
        LEFT JOIN
            read_parquet('%DATADIR%division_area.geoparquet') AS b
        ON
            ST_Intersects(a.geom, b.geometry)
        WHERE
            b.subtype = 'locality'
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
) TO '%DATADIR%geocodeur_infra.parquet' (FORMAT 'PARQUET');
`

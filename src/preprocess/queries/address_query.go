/*
describe select * from read_parquet('address.geoparquet');
┌────────────────┬────────────────────────────────────────────────────────────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
│  column_name   │                                      column_type                                       │  null   │   key   │ default │  extra  │
│    varchar     │                                        varchar                                         │ varchar │ varchar │ varchar │ varchar │
├────────────────┼────────────────────────────────────────────────────────────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ id             │ VARCHAR                                                                                │ YES     │         │         │         │
│ geometry       │ BLOB                                                                                   │ YES     │         │         │         │
│ bbox           │ STRUCT(xmin FLOAT, xmax FLOAT, ymin FLOAT, ymax FLOAT)                                 │ YES     │         │         │         │
│ country        │ VARCHAR                                                                                │ YES     │         │         │         │
│ postcode       │ VARCHAR                                                                                │ YES     │         │         │         │
│ street         │ VARCHAR                                                                                │ YES     │         │         │         │
│ number         │ VARCHAR                                                                                │ YES     │         │         │         │
│ unit           │ VARCHAR                                                                                │ YES     │         │         │         │
│ address_levels │ STRUCT("value" VARCHAR)[]                                                              │ YES     │         │         │         │
│ version        │ INTEGER                                                                                │ YES     │         │         │         │
│ sources        │ STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, update_time VARCHAR, co…  │ YES     │         │         │         │
├────────────────┴────────────────────────────────────────────────────────────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
│ 11 rows                                                                                                                               6 columns │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
*/

package queries

var AddressQuery = `
INSTALL spatial;
LOAD spatial;

COPY (
 	WITH clip AS (
        SELECT
            CASE
            WHEN '%COUNTRY%' != '' THEN (SELECT geometry from read_parquet('%DATADIR%division_area.geoparquet') where lower(names.primary) = '%COUNTRY%' and subtype = 'country')
            ELSE ST_GeomFromText('POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')
            END AS geom
    )
    SELECT
		a.id,
		a.street || ' ' || a.number AS name,
		ST_AsText(a.geometry) AS geom,
		'address' as class,
		'address' as subclass,
		array_to_string([x.value for x in address_levels], ';') as relation
	FROM
		read_parquet('%DATADIR%address.geoparquet') AS a, clip AS b
	WHERE
		ST_Within(a.geometry, b.geom)
) TO '%DATADIR%geocodeur_address.parquet' (FORMAT 'PARQUET');
`

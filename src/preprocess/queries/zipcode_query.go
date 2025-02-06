package queries

var ZipcodeQuery = `
INSTALL spatial;
LOAD spatial;

COPY (
 	WITH clip AS (
        SELECT
            CASE
            WHEN '%COUNTRY%' != '' THEN (SELECT geometry from read_parquet('%DATADIR%division_area.geoparquet') where lower(names.primary) = '%COUNTRY%' and subtype = 'country')
            ELSE ST_GeomFromText('POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')
            END AS geom
    ),
	zips AS (
		SELECT
			ANY_VALUE(id) as id,
			postcode as zipcode,
			ST_ConvexHull(ST_Union_Agg(geometry)) AS geom
		FROM
			read_parquet('%DATADIR%address.geoparquet')
		GROUP BY
			postcode
	)
	SELECT
		a.id,
		a.zipcode AS name,
		ST_AsGeoJSON(a.geom) AS geom,
		ST_AsText(ST_Centroid(a.geom)) AS centroid,
		'zipcode' as class,
		'zipcode' as subclass,
		NULL::VARCHAR as relation
	FROM
		zips AS a, clip AS b
	WHERE
		ST_Within(a.geom, b.geom)
) TO '%DATADIR%geocodeur_zipcode.parquet' (FORMAT 'PARQUET');
`

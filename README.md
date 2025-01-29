# geocodeur

This project explores the creation of a geocoder using Overture Maps data with free-text search functionality, leveraging FTS and similarity search based on trigram matching. The goal is not to replicate the original Overture data but to design a simplified schema focused solely on geocoding needs, avoiding unnecessary data that does not serve this purpose.

The geocoder will include the following data categories:

- Division
- Road
- POI
- Water
- Infrastructure

To improve search precision, multiple aliases can be generated for each Overture Maps feature. These aliases anticipate user input that may combine multiple locations to refine search results. For example, in the Netherlands, many streets are named "Kerkstraat." If a user searches for "Kerkstraat Amsterdam," the geocoder should prioritize "Kerkstraat" in Amsterdam as the top result. To achieve this, aliases like "Kerkstraat," "Kerkstraat {intersecting division.locality}," and "Kerkstraat {intersecting division.county}" are added. These aliases vary based on the class and subclass of the feature.

Postgres Full Text Search (FTS) is used to index the aliases and can handle most of the queries efficiently. For instance if a user types "Kerkstr Amsterd," the geocoder can still locate "Kerkstraat" in Amsterdam. When FTS is not able to find a match, trigram matching takes over to find similar results. This approach is more tolerant of typos.

Additionally, related segments for road and water are merged into a single entry, enabling retrieval of the full feature rather than fragmented segments in the Overture Maps data. This approach reduces the likelihood of excessive high-matching results for the same road or water.

Other cases can also be accommodated. For instance:

A user searching for "A2" (a highway in the Netherlands) can find the correct result even though its name in Overture is "Rijksweg A2," thanks to aliases like "A2" and "Rijksweg A2."
For entries with names like "'s-Hertogenbosch," a common alias "den bosch" can be added, as users are more likely to type the latter. These aliases are applied to all related entries and relationships.

## Vectors?

A test was also done with creating vectors for the aliases instead of FTS and trigram matching but since the input will not benefit much from semantics we can stick with FTS and trigram matching. Some cons of vectors for the geocoder:

- Much more computation time to create the database
- Much more storage space needed for the vectors, With a small test dataset 96MB vs 11MB
- Cannot handle typos as well.
- Does not handle swappable words as well as trigram matching.
- Slower query times vs FTS, rougly the same for trigram matching.

## ToDo

This is a first experiment and seems to work pretty good but there are still some todo's.

- API: OpenAPI spec
- API: Endpoint for reverse geocoding
- API: Filter results based on bbox
- Data: Add infrastructure data
- Data: Add address data
- Data: Store original overture id's in the overture table
- Data: Some problems and todo's described below
- CLI: Make it easier to setup geocodeur

## Getting started

### Download & prepare data

To download data we can use the overturemaps CLI tool and to process the data we use DuckDB. To install the CLI tool we can use pip.

```sh
pip install overturemaps
```

To install DuckDB we can use the following commands.

```sh
curl --fail --location --progress-bar --output duckdb_cli-linux-amd64.zip https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip
```

Now we can download all data from Overture Maps with a given bounding box using the `download` script. The script will download all data in the bounding box and store it in the `data/download` directory.

```sh
# Test set 1
./scripts/download.sh 5.117491,51.598439,5.579449,51.821835

# Test set 2 (1/6th of the Netherlands)
./scripts/download.sh 4.60788273,51.5727799,6.12797006,52.1129134

# Test set 3 (Netherlands and big part of Belgium and small patch of Germany)
./scripts/download.sh 3.1624817624420167,50.76012028429577,7.274625587715649,53.50694358074323
```

We can now process the download Overture Maps data to make it usable for geocodeur.

```sh
go run main.go process
```

### Load data into the database

Start a local PostGIS database

```sh
docker compose up -d
```

Create the tables and load data

```sh
go run main.go create
```

### Start API

When data is loaded in the database we can start the API server and fire some queries.

```sh
go run main.go server
```

#### Docs

OpenAPI docs available at [http://localhost:8080/docs](http://localhost:8080/docs)

#### Query API

```sh
curl -X GET "http://localhost:8080/geocode?q=Adr%20poorters%20Vught&class=road&limit=10"
```

FTS has a 1 result so no fallback to trigram matching is needed.

```json
{
  "ms": 3,
  "results": [
    {
      "name": "Adriaan Poortersstraat",
      "class": "road",
      "subclass": "residential",
      "divisions": "Vught",
      "alias": "adriaan poortersstraat vught",
      "searchType": "fts",
      "similarity": 0.548,
      "geom": {
        "type": "LineString",
        "coordinates": [
          [5.2859974, 51.6466151],
          [5.2860828, 51.646718],
          [5.2891755, 51.6474486]
        ]
      }
    }
  ]
}
```

## Data

### Database

The database consists of 2 tables: `overture` and `overture_search`. The `overture` table contains the features from Overture Maps and the `overture_search` table contains aliases for the features which point to the `overture` table. The column `alias` in the `overture_search` table has a `gin_trgm_ops` index on it for fast searching using the PostgreSQL extension `pg_trgm` and another index on alias also using gin but with `to_tsvector` on `alias` for FTS.

![example](./static/example.jpg)

### Division

- Add locality for neighbourhoods features
- Add county for locality features

### Road

- Only segments with a primary name, we cannot search for a segment without a name so leave them out.
- Only segments with a subtype road. Tracks are not usefull for geocoding and water we will get from a different source since water features are segments and not water bodies.
- Roads are split up in multiple segments merge them by name, class (overture) and group created from union of buffered roads.
- Add relations for locality and county to roads but exlude realtions for motorways since this does not make much sense.

#### Solve

- Motorways are a big mess, inconsistent naming and alot of segments without a name
- Subtypes are not always that good, for instance we have a residential road with a road that should connect to it with the same name but the segment is unclassified resulting in 2 roads.
- Sometimes roads are grouped but there is another road in between, should this be 1 road or 2 roads?

### Water

- Only water with primary name
- Subtype is most of the time the same as class and not helpfull use subtype as subclass
- Features with lines are sometimes split up and also can represent the same feature, these need to be grouped and merged
- Polygons are not directly split up but need to be grouped aswell when close and representing the same feature

#### Solve

We have features 'duplicated' as lines and polygons, remove a line if it's within a polygon with the same name and subclass

### POI

- Take all pois with confidence 0.4 or higher
- Add locality relation to pois

## Building executable

Manually build the geocodeur executable with the following command.

```sh
go build -ldflags="-s -w" -gcflags="-m" -o geocodeur ./src/main.go
```

## Docker

Run geocodeur server and mount a config file, we use `--network host` so geocodeur can connect directly to the database.
Latest image is available on ghcr.io.

```sh
docker run --network host -v ./config/geocodeur.conf:/config/geocodeur.conf ghcr.io/tebben/geocodeur:latest
```

#!/bin/bash

echo "Processing division data"
duckdb < ./queries/division.sql

echo "Processing road data"
duckdb < ./queries/road.sql

echo "Processing POI data"
duckdb < ./queries/poi.sql

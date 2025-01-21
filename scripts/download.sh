#!/bin/bash

# Set the local directory path
download_directory="./data/download"

# Check if the bounding box is provided as an argument
if [ $# -lt 1 ]; then
  echo "Usage: $0 <bbox>"
  exit 1
fi

# Get the bbox from the first argument
bbox="$1"

# Create the local directory if it doesn't exist
if [ ! -d "$download_directory" ]; then
  mkdir -p "$download_directory"
  echo "Created directory: $download_directory"
fi

echo "Downloading division data"
overturemaps download --bbox "$bbox" -t division_area -f geoparquet -o "$download_directory/division_area.geoparquet"

echo "Downloading road data"
overturemaps download --bbox "$bbox" -t segment -f geoparquet -o "$download_directory/segment.geoparquet"

echo "Downloading POI data"
overturemaps download --bbox "$bbox" -t place -f geoparquet -o "$download_directory/place.geoparquet"

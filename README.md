# ckanext-shptoapi

CKAN extension (2.9+ and tested on 2.10) that ingests uploaded shapefiles (ZIP), reprojects them to EPSG:4326, loads them into PostGIS, and exposes GeoJSON endpoints compatible with TerriaJS.

## Requirements
- CKAN 2.9+ with Python 3.
- PostGIS reachable from the CKAN host.
- GDAL with `ogr2ogr` and `ogrinfo` available on PATH.
- Optional: `pyproj` to improve EPSG detection from `.prj`.

## Installation
```
pip install -e .
```
Enable the plugin:
```
ckan.plugins = ... shptoapi
```

## Configuration
All options have safe defaults:
- `ckanext.shptoapi.enabled`: turn the extension on (default `false`).
- `ckanext.shptoapi.auto_process`: `true` to auto-process every shapefile ZIP; keep `false` for opt-in per resource (default).
- `ckanext.shptoapi.flag_extra`: resource extra that toggles processing (`vector_enabled`).
- `ckanext.shptoapi.max_size_mb`: max ZIP size (default 200).
- `ckanext.shptoapi.max_features`: feature limit before rejecting (default 50000).
- `ckanext.shptoapi.max_items`: cap for `limit` in items endpoint (default 1000).
- `ckanext.shptoapi.schema`: destination schema in PostGIS (default `public`).
- `ckanext.shptoapi.table_prefix`: prefix for target table (`vector_`).
- `ckanext.shptoapi.ogr_pg_dsn`: DSN for `ogr2ogr` (e.g., `host='db' dbname='ckan' user='ckan' password='...'`). Falls back to `ckan.datastore.write_url` or `sqlalchemy.url` when omitted.
- `ckanext.shptoapi.cors_origin`: allowed origin for CORS (default `*`).

### Per-resource panel (manual activation)
- Set `ckanext.shptoapi.enabled=true` and keep `auto_process=false`.
- Visit `/vector/<resource_id>/panel` (requires permission to update the resource):
  - Enable: sets `vector_enabled=true` and runs validation, reprojection, and PostGIS load.
  - Disable: sets `vector_enabled=false`, drops the spatial table, and clears vector metadata extras.
  - If the table is missing (dropped manually), visiting the panel or calling the endpoints will reprocess the resource automatically when the flag is active and the plugin is enabled.

You can also skip the panel and set `vector_enabled=true` directly (e.g., via scheming). The plugin will process the resource on `after_update`.

## Processing flow
1. Validate ZIP contains `.shp`, `.shx`, `.dbf`, `.prj` and respects `max_size_mb`.
2. Extract in a secure temp directory (Zip Slip protection).
3. Read `.prj` to detect CRS (reject when missing).
4. Count features with `ogrinfo` and check against `max_features`.
5. Run `ogr2ogr -t_srs EPSG:4326` into `schema.table_prefix<resource_id>` in PostGIS.
6. Create GIST spatial index; compute `bbox`, `geom_type`, `feature_count`.
7. Persist extras on the resource: `vector_table`, `vector_schema`, `srid`, `bbox`, `geom_type`, `feature_count`.
8. On resource delete, the associated table is dropped.

## Terria-friendly API
Flask routes exposed by the plugin:
- `GET /vector/<resource_id>/metadata`  
  Response: `{"bbox":[minx,miny,maxx,maxy],"geom_type":"MULTIPOLYGON","feature_count":123,"srid":4326,"vector_table":"public.vector_<id>"}`
- `GET /vector/<resource_id>/items?bbox=minx,miny,maxx,maxy&limit=500&offset=0`  
  Returns GeoJSON `FeatureCollection`. Honors `limit` (capped by `max_items`) and bbox filter.

### Example `items` response
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Polygon", "coordinates": [...]},
      "properties": {"ogc_fid": 1, "name": "A"}
    }
  ]
}
```

## TerriaJS example
Add to `catalog`:
```json
{
  "type": "geojson",
  "name": "CKAN layer",
  "url": "https://your-ckan.org/vector/<resource_id>/items",
  "disablePreview": false,
  "rectangle": {
    "west": -180,
    "south": -90,
    "east": 180,
    "north": 90
  }
}
```
You can obtain `rectangle` using `/vector/<resource_id>/metadata` (`bbox`).

## Security considerations
- Zip Slip protection during extraction.
- Limits on ZIP size and feature count.
- Rejects uploads without `.prj` or unknown CRS.
- Clear errors surfaced as `ValidationError` on resource create/update.
- CORS enabled on `/vector/*` endpoints.

## Quick testing
- The plugin implements `IResourceController` and `IBlueprint`; it adds only the vector panel template.
- Upload a shapefile ZIP with `vector_enabled=true` and check extras plus the table in PostGIS (`public.vector_<id>`).

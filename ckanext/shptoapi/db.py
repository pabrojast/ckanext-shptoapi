import json
import re
from typing import Dict, Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ckan import model
from ckan.plugins import toolkit

from ckanext.shptoapi import log
from ckanext.shptoapi.errors import ShpToApiError

SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(value: str, kind: str) -> str:
    if not value or not SAFE_NAME.match(value):
        raise ShpToApiError(f"Invalid {kind} name: {value}")
    return value


def build_full_table(schema: Optional[str], table: str) -> str:
    table = _safe_identifier(table, "table")
    if schema:
        schema = _safe_identifier(schema, "schema")
        return f"{schema}.{table}"
    return table


def parse_extent(extent: Optional[str]) -> Optional[List[float]]:
    if not extent:
        return None
    # BOX(minx miny,maxx maxy)
    try:
        bounds = extent.strip().replace("BOX(", "").replace(")", "")
        first, second = bounds.split(",")
        minx, miny = [float(v) for v in first.split()]
        maxx, maxy = [float(v) for v in second.split()]
    except Exception:
        log.exception("Could not parse ST_Extent: %s", extent)
        return None
    return [minx, miny, maxx, maxy]


def fetch_metadata(schema: Optional[str], table: str) -> Dict:
    full_table = build_full_table(schema, table)
    try:
        with model.meta.engine.begin() as conn:
            extent = conn.execute(
                text(f"SELECT ST_Extent(geom) AS extent FROM {full_table}")
            ).scalar()
            geom_type = conn.execute(
                text(
                    f"SELECT GeometryType(geom) FROM {full_table} "
                    "WHERE geom IS NOT NULL LIMIT 1"
                )
            ).scalar()
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {full_table}")
            ).scalar()
    except SQLAlchemyError as exc:
        raise ShpToApiError(f"Could not read spatial metadata: {exc}") from exc

    bbox = parse_extent(extent)
    return {
        "bbox": bbox,
        "geom_type": geom_type,
        "feature_count": count,
    }


def fetch_features(
    schema: Optional[str],
    table: str,
    bbox: Optional[Iterable[float]],
    limit: int,
    offset: int,
) -> List[Dict]:
    full_table = build_full_table(schema, table)
    params = {"limit": limit, "offset": offset}
    clauses = []
    if bbox:
        params.update(
            {"minx": bbox[0], "miny": bbox[1], "maxx": bbox[2], "maxy": bbox[3]}
        )
        clauses.append(
            "geom && ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326)"
        )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = text(
        f"""
        SELECT ST_AsGeoJSON(geom) AS geom, to_jsonb(t) - 'geom' AS props
        FROM {full_table} AS t
        {where}
        LIMIT :limit OFFSET :offset
        """
    )
    try:
        with model.meta.engine.begin() as conn:
            rows = conn.execute(sql, params).fetchall()
    except SQLAlchemyError as exc:
        raise ShpToApiError(f"Could not read features: {exc}") from exc

    features: List[Dict] = []
    for row in rows:
        props = row.props
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except Exception:
                pass
        geom = row.geom
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(geom) if geom else None,
                "properties": props,
            }
        )
    return features


def ensure_spatial_index(schema: Optional[str], table: str) -> None:
    full_table = build_full_table(schema, table)
    index_name = _safe_identifier(f"{table}_geom_gist", "index")
    sql = text(
        f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table} "
        "USING GIST (geom)"
    )
    analyze_sql = text(f"ANALYZE {full_table}")
    with model.meta.engine.begin() as conn:
        conn.execute(sql)
        conn.execute(analyze_sql)


def drop_table(schema: Optional[str], table: str) -> None:
    full_table = build_full_table(schema, table)
    with model.meta.engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE"))


def enforce_srid(schema: Optional[str], table: str, srid: int) -> None:
    full_table = build_full_table(schema, table)
    sql = text(
        f"ALTER TABLE {full_table} "
        "ALTER COLUMN geom TYPE geometry(Geometry, :srid) "
        "USING ST_SetSRID(geom, :srid)"
    )
    with model.meta.engine.begin() as conn:
        conn.execute(sql, {"srid": srid})


def table_exists(schema: Optional[str], table: str) -> bool:
    full_table = build_full_table(schema, table)
    sql = text("SELECT to_regclass(:name)")
    with model.meta.engine.begin() as conn:
        result = conn.execute(sql, {"name": full_table}).scalar()
    return result is not None

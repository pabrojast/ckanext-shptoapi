import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from typing import Dict, Iterable, Optional

from sqlalchemy.engine.url import make_url

from ckan import model
from ckan.lib import uploader
from ckan.plugins import toolkit

from ckanext.shptoapi import log
from ckanext.shptoapi import db
from ckanext.shptoapi.errors import ShpToApiError

REQUIRED_EXTENSIONS = {".shp", ".shx", ".dbf", ".prj"}


def process_resource(resource, context: Dict, config: Dict) -> Optional[Dict]:
    """Process an uploaded resource and load its shapefile into PostGIS."""
    resource_dict = _resource_dict(resource, context)
    if not _should_process(resource_dict, config):
        return None

    zip_path = _get_file_path(resource_dict)
    _validate_size(zip_path, config)

    schema = config.get("ckanext.shptoapi.schema", "public")
    table_prefix = config.get("ckanext.shptoapi.table_prefix", "vector_")
    max_features = int(config.get("ckanext.shptoapi.max_features", 50000))
    ogr_dsn = config.get("ckanext.shptoapi.ogr_pg_dsn") or _build_pg_dsn(config)
    table_name = _build_table_name(resource_dict["id"], table_prefix)

    log.info("Processing shapefile for resource %s", resource_dict["id"])

    with tempfile.TemporaryDirectory(prefix="shptoapi_") as tmpdir:
        shapefile_parts = _extract_and_validate(zip_path, tmpdir)
        srid = _detect_srid(shapefile_parts["prj"])
        feature_count = _feature_count(shapefile_parts["shp"])
        if feature_count is not None and feature_count > max_features:
            raise ShpToApiError(
                f"Shapefile has {feature_count} features and exceeds the limit "
                f"({max_features})."
            )
        _load_to_postgis(
            shapefile_parts["shp"],
            ogr_dsn,
            schema,
            table_name,
            srid,
        )
        db.enforce_srid(schema, table_name, 4326)
        db.ensure_spatial_index(schema, table_name)
        metadata = db.fetch_metadata(schema, table_name)
        if metadata.get("feature_count") and metadata["feature_count"] > max_features:
            db.drop_table(schema, table_name)
            raise ShpToApiError(
                f"Resulting table has {metadata['feature_count']} features and "
                f"exceeds the allowed limit ({max_features})."
            )
        if feature_count is not None:
            metadata["feature_count"] = feature_count
        metadata.update(
            {
                "srid": 4326,
                "vector_table": db.build_full_table(schema, table_name),
                "vector_schema": schema,
            }
        )
        _update_extras(resource_dict, metadata, context, config)
        return metadata


def _resource_dict(resource, context: Dict) -> Dict:
    rid = resource["id"] if isinstance(resource, dict) else resource.id
    ctx = {
        "model": model,
        "session": model.Session,
        "ignore_auth": True,
        "user": context.get("user") if context else None,
    }
    return toolkit.get_action("resource_show")(ctx, {"id": rid})


def _extras_lookup(resource_dict: Dict, key: str) -> Optional[str]:
    extras_dict = _extras_to_dict(resource_dict.get("extras") or [])
    return extras_dict.get(key)


def _asbool(value: Optional[str]) -> bool:
    return str(value).lower() in ("1", "true", "yes", "on")


def _should_process(resource_dict: Dict, config: Dict) -> bool:
    if not toolkit.asbool(config.get("ckanext.shptoapi.enabled", False)):
        return False
    if resource_dict.get("url_type") != "upload":
        return False
    format_value = (resource_dict.get("format") or "").lower()
    if format_value not in ("shp", "shapefile", "zip"):
        filename = resource_dict.get("url", "")
        if not filename.lower().endswith(".zip"):
            return False

    extra_flag = config.get("ckanext.shptoapi.flag_extra", "vector_enabled")
    enabled_extra = _extras_lookup(resource_dict, extra_flag)
    auto_process = toolkit.asbool(config.get("ckanext.shptoapi.auto_process", False))
    if not auto_process and not _asbool(enabled_extra):
        return False
    return True


def _get_file_path(resource_dict: Dict) -> str:
    upload = uploader.get_resource_uploader(resource_dict)
    if hasattr(upload, "get_path"):
        path = upload.get_path(resource_dict["id"])
    elif hasattr(upload, "path"):
        path = upload.path
    else:
        raise ShpToApiError("Uploaded file location could not be determined.")
    if not path or not os.path.exists(path):
        raise ShpToApiError("The resource file does not exist on disk.")
    if not path.lower().endswith(".zip"):
        raise ShpToApiError("Resource must be a ZIP containing a shapefile.")
    return path


def _validate_size(path: str, config: Dict) -> None:
    max_mb = int(config.get("ckanext.shptoapi.max_size_mb", 200))
    size_mb = os.path.getsize(path) / (1024 * 1024)
    if size_mb > max_mb:
        raise ShpToApiError(
            f"ZIP size is {size_mb:.1f} MB and exceeds the configured limit ({max_mb} MB)."
        )


def _extract_and_validate(zip_path: str, target_dir: str) -> Dict[str, str]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = os.path.normpath(member.filename)
            if member_path.startswith("..") or os.path.isabs(member_path):
                raise ShpToApiError("Invalid ZIP: contains unsafe paths.")
            dest_path = os.path.abspath(os.path.join(target_dir, member_path))
            base_dir = os.path.abspath(target_dir)
            if not dest_path.startswith(base_dir):
                raise ShpToApiError("Invalid ZIP: Zip Slip attempt detected.")
        zf.extractall(target_dir)

    shapefile = _find_shapefile(target_dir)
    if not shapefile:
        raise ShpToApiError(
            "ZIP must include .shp, .shx, .dbf and .prj files sharing the same name."
        )
    return shapefile


def _find_shapefile(root_dir: str) -> Optional[Dict[str, str]]:
    for dirpath, _, filenames in os.walk(root_dir):
        lower_files = {name.lower(): name for name in filenames}
        for name in filenames:
            if not name.lower().endswith(".shp"):
                continue
            base = os.path.splitext(name)[0]
            expected = {f"{base}{ext}".lower() for ext in REQUIRED_EXTENSIONS}
            if expected.issubset(set(f.lower() for f in filenames)):
                return {
                    "shp": os.path.join(dirpath, name),
                    "shx": os.path.join(dirpath, lower_files[f"{base}.shx".lower()]),
                    "dbf": os.path.join(dirpath, lower_files[f"{base}.dbf".lower()]),
                    "prj": os.path.join(dirpath, lower_files[f"{base}.prj".lower()]),
                }
    return None


def _detect_srid(prj_path: str) -> int:
    with open(prj_path, "r", encoding="utf-8", errors="ignore") as prj_file:
        content = prj_file.read()
    match = re.search(r"AUTHORITY\\[\"EPSG\",\"(\\d+)\"\\]", content, re.IGNORECASE)
    if match:
        return int(match.group(1))
    try:
        from pyproj import CRS  # type: ignore

        epsg = CRS.from_wkt(content).to_epsg()
        if epsg:
            return int(epsg)
    except Exception:
        pass
    raise ShpToApiError("CRS could not be detected from the .prj file.")


def _feature_count(shp_path: str) -> Optional[int]:
    if shutil.which("ogrinfo") is None:
        log.warning("ogrinfo is not available; skipping feature count.")
        return None
    try:
        result = subprocess.run(
            ["ogrinfo", "-so", "-al", shp_path],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        log.warning("Could not read feature count with ogrinfo: %s", exc.stderr)
        return None
    for line in result.stdout.splitlines():
        if "Feature Count" in line:
            try:
                return int(line.split(":")[1].strip())
            except Exception:
                continue
    return None


def _build_table_name(resource_id: str, prefix: str) -> str:
    safe_id = re.sub(r"[^A-Za-z0-9]", "", resource_id)
    if not safe_id:
        safe_id = "resource"
    table = f"{prefix}{safe_id}".lower()
    return table[:60]


def _quote(value: Optional[str]) -> str:
    if value is None:
        return ""
    escaped = str(value).replace("'", "\\'")
    return f"'{escaped}'"


def _build_pg_dsn(config: Dict) -> str:
    conn_str = (
        config.get("ckan.datastore.write_url")
        or config.get("sqlalchemy.url")
        or ""
    )
    if not conn_str:
        raise ShpToApiError(
            "No connection string found. Configure ckanext.shptoapi.ogr_pg_dsn "
            "or ckan.datastore.write_url."
        )
    url = make_url(conn_str)
    parts = [
        f"host={_quote(url.host)}" if url.host else None,
        f"port={url.port}" if url.port else None,
        f"dbname={_quote(url.database)}" if url.database else None,
        f"user={_quote(url.username)}" if url.username else None,
        f"password={_quote(url.password)}" if url.password else None,
    ]
    return " ".join(p for p in parts if p)


def _load_to_postgis(
    shp_path: str,
    ogr_pg_dsn: str,
    schema: Optional[str],
    table_name: str,
    source_srid: int,
) -> None:
    if shutil.which("ogr2ogr") is None:
        raise ShpToApiError("ogr2ogr is not available in PATH.")
    target_table = db.build_full_table(schema, table_name)
    cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        f"PG:{ogr_pg_dsn}",
        shp_path,
        "-nln",
        target_table,
        "-lco",
        "GEOMETRY_NAME=geom",
        "-lco",
        "FID=ogc_fid",
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-t_srs",
        "EPSG:4326",
        "-overwrite",
    ]
    if source_srid:
        cmd.extend(["-s_srs", f"EPSG:{source_srid}"])
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        raise ShpToApiError(
            f"Loading to PostGIS failed ({exc.returncode}): {exc.stderr}"
        ) from exc


def _update_extras(resource_dict: Dict, metadata: Dict, context: Dict, config: Dict) -> None:
    bbox_value = metadata.get("bbox")
    schema_value = metadata.get("vector_schema") or ""
    flag_name = config.get("ckanext.shptoapi.flag_extra", "vector_enabled")
    existing_extras = _extras_to_dict(resource_dict.get("extras") or [])
    flag_value = existing_extras.get(flag_name)

    updates = {
        "vector_table": metadata.get("vector_table"),
        "vector_schema": schema_value,
        "srid": str(metadata.get("srid") or ""),
        "bbox": json.dumps(bbox_value) if bbox_value else "",
        "geom_type": metadata.get("geom_type") or "",
        "feature_count": str(metadata.get("feature_count") or ""),
    }
    existing_extras.update(updates)
    if flag_value is not None:
        existing_extras[flag_name] = flag_value

    ctx = {
        "model": model,
        "session": model.Session,
        "ignore_auth": True,
        "user": context.get("user") if context else None,
    }
    extras_payload = _extras_to_list(existing_extras)
    toolkit.get_action("resource_patch")(
        ctx, {"id": resource_dict["id"], "extras": extras_payload}
    )


def _extras_to_dict(extras) -> Dict[str, str]:
    if isinstance(extras, dict):
        return dict(extras)
    data: Dict[str, str] = {}
    for item in extras:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if key is not None:
            data[key] = item.get("value")
    return data


def _extras_to_list(extras_dict: Dict[str, str]):
    return [{"key": k, "value": v} for k, v in extras_dict.items()]


def set_resource_flag(resource_id: str, enabled: bool, user: Optional[str], config: Dict) -> bool:
    flag_name = config.get("ckanext.shptoapi.flag_extra", "vector_enabled")
    ctx = {
        "model": model,
        "session": model.Session,
        "user": user,
    }
    resource = toolkit.get_action("resource_show")(ctx, {"id": resource_id})
    extras = _extras_to_dict(resource.get("extras") or [])
    extras[flag_name] = "true" if enabled else "false"
    toolkit.get_action("resource_patch")(
        ctx, {"id": resource_id, "extras": _extras_to_list(extras)}
    )
    return enabled


def clear_vector_metadata(resource_id: str, user: Optional[str]) -> None:
    ctx = {
        "model": model,
        "session": model.Session,
        "user": user,
    }
    resource = toolkit.get_action("resource_show")(ctx, {"id": resource_id})
    extras = _extras_to_dict(resource.get("extras") or [])
    for key in ["vector_table", "vector_schema", "srid", "bbox", "geom_type", "feature_count"]:
        extras.pop(key, None)
    toolkit.get_action("resource_patch")(
        ctx, {"id": resource_id, "extras": _extras_to_list(extras)}
    )

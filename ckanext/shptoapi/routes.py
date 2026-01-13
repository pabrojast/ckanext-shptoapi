import json
from typing import Dict, Optional

from flask import Blueprint, Response, request, g, redirect

from ckan import model
from ckan.plugins import toolkit

from ckanext.shptoapi import log
from ckanext.shptoapi import db, logic
from ckanext.shptoapi.errors import ShpToApiError


def create_blueprint(config: Dict) -> Blueprint:
    bp = Blueprint("shptoapi", __name__)

    @bp.after_request
    def add_cors_headers(response: Response) -> Response:
        allow_origin = config.get("ckanext.shptoapi.cors_origin", "*")
        response.headers.setdefault("Access-Control-Allow-Origin", allow_origin)
        response.headers.setdefault(
            "Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept"
        )
        response.headers.setdefault("Access-Control-Allow-Methods", "GET, OPTIONS")
        return response

    @bp.route("/vector/<resource_id>/metadata", methods=["GET"])
    def metadata(resource_id: str):
        try:
            resource, context = _load_resource(resource_id)
            resource = _ensure_vector_ready(resource, context, config)
            table_info = _vector_info(resource)
            metadata = db.fetch_metadata(
                table_info.get("schema"), table_info["table"]
            )
            metadata.update(
                {
                    "vector_table": table_info["full_table"],
                    "srid": int(table_info.get("srid") or 4326),
                    "feature_count": int(table_info.get("feature_count") or metadata["feature_count"] or 0),
                }
            )
            return _json_response(metadata)
        except toolkit.NotAuthorized:
            return _error_response("Not authorized", 403)
        except ShpToApiError as exc:
            return _error_response(str(exc), 400)
        except toolkit.ObjectNotFound:
            return _error_response("Resource not found", 404)
        except Exception:
            log.exception("Error retrieving metadata for %s", resource_id)
            return _error_response("Internal error", 500)

    @bp.route("/vector/<resource_id>/items", methods=["GET"])
    def items(resource_id: str):
        try:
            resource, context = _load_resource(resource_id)
            resource = _ensure_vector_ready(resource, context, config)
            table_info = _vector_info(resource)
            bbox = _parse_bbox_param(request.args.get("bbox"))
            limit = _read_int(request.args.get("limit"), default=100)
            offset = _read_int(request.args.get("offset"), default=0)
            max_limit = int(config.get("ckanext.shptoapi.max_items", 1000))
            if limit > max_limit:
                limit = max_limit
            features = db.fetch_features(
                table_info.get("schema"), table_info["table"], bbox, limit, offset
            )
            payload = {"type": "FeatureCollection", "features": features}
            return _json_response(payload)
        except toolkit.NotAuthorized:
            return _error_response("Not authorized", 403)
        except ShpToApiError as exc:
            return _error_response(str(exc), 400)
        except toolkit.ObjectNotFound:
            return _error_response("Resource not found", 404)
        except Exception:
            log.exception("Error retrieving features for %s", resource_id)
            return _error_response("Internal error", 500)

    @bp.route("/vector/<resource_id>/metadata", methods=["OPTIONS"])
    @bp.route("/vector/<resource_id>/items", methods=["OPTIONS"])
    def options(resource_id: str):
        return _json_response({}, status=204)

    @bp.route("/vector/<resource_id>/panel", methods=["GET", "POST"])
    def panel(resource_id: str):
        flag_name = config.get("ckanext.shptoapi.flag_extra", "vector_enabled")
        try:
            if request.method == "POST":
                resource, context = _load_resource(resource_id, for_update=True)
                action = request.form.get("action")
                if action == "enable":
                    logic.set_resource_flag(resource_id, True, context.get("user"), config)
                    logic.process_resource(resource, context, config)
                elif action == "disable":
                    logic.set_resource_flag(resource_id, False, context.get("user"), config)
                    _drop_vector_table(resource)
                    logic.clear_vector_metadata(resource_id, context.get("user"))
                else:
                    raise ShpToApiError("Invalid action.")
                return redirect(f"/vector/{resource_id}/panel")

            resource, context = _load_resource(resource_id, for_update=False)
            resource = _ensure_vector_ready(resource, context, config)
            enabled = _flag_enabled(resource, flag_name)
            metadata = None
            try:
                table_info = _vector_info(resource)
                metadata = db.fetch_metadata(
                    table_info.get("schema"), table_info["table"]
                )
                metadata.update(
                    {
                        "vector_table": table_info["full_table"],
                        "srid": int(table_info.get("srid") or 4326),
                        "feature_count": int(
                            table_info.get("feature_count") or metadata["feature_count"] or 0
                        ),
                    }
                )
            except Exception:
                metadata = None
            return toolkit.render(
                "shptoapi/panel.html",
                {
                    "resource": resource,
                    "flag_name": flag_name,
                    "enabled": enabled,
                    "metadata": metadata,
                },
            )
        except toolkit.NotAuthorized:
            return _error_response("Not authorized", 403)
        except toolkit.ObjectNotFound:
            return _error_response("Resource not found", 404)
        except ShpToApiError as exc:
            return _error_response(str(exc), 400)
        except Exception:
            log.exception("Error in panel for %s", resource_id)
            return _error_response("Internal error", 500)

    return bp


def _json_response(data: Dict, status: int = 200) -> Response:
    response = Response(json.dumps(data), status=status, mimetype="application/json")
    return response


def _error_response(message: str, status: int) -> Response:
    return _json_response({"error": message}, status=status)


def _load_resource(resource_id: str, for_update: bool = False):
    context = {
        "model": model,
        "session": model.Session,
        "user": getattr(g, "user", None),
    }
    action = "resource_update" if for_update else "resource_show"
    toolkit.check_access(action, context, {"id": resource_id})
    resource = toolkit.get_action("resource_show")(context, {"id": resource_id})
    return resource, context


def _vector_info(resource: Dict) -> Dict:
    extras = resource.get("extras") or []
    if isinstance(extras, dict):
        extras = [{"key": k, "value": v} for k, v in extras.items()]

    def _get(key: str) -> Optional[str]:
        for item in extras:
            if item.get("key") == key:
                return item.get("value")
        return None

    vector_table = _get("vector_table")
    if not vector_table:
        raise ShpToApiError("Resource does not have vector_table in extras.")
    schema = None
    table = vector_table
    if "." in vector_table:
        schema, table = vector_table.split(".", 1)
    srid = _get("srid") or "4326"
    feature_count = _get("feature_count")
    return {
        "schema": schema,
        "table": table,
        "full_table": vector_table,
        "srid": srid,
        "feature_count": feature_count,
    }


def _parse_bbox_param(raw: Optional[str]) -> Optional[list]:
    if not raw:
        return None
    parts = raw.split(",")
    if len(parts) != 4:
        raise ShpToApiError("bbox must use minx,miny,maxx,maxy")
    try:
        return [float(p) for p in parts]
    except ValueError as exc:
        raise ShpToApiError("bbox contains non numeric values") from exc


def _read_int(raw: Optional[str], default: int) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _ensure_vector_ready(resource: Dict, context: Dict, config: Dict) -> Dict:
    if not toolkit.asbool(config.get("ckanext.shptoapi.enabled", False)):
        return resource
    flag_name = config.get("ckanext.shptoapi.flag_extra", "vector_enabled")
    if not _flag_enabled(resource, flag_name):
        return resource

    try:
        info = _vector_info(resource)
        if db.table_exists(info.get("schema"), info["table"]):
            return resource
    except ShpToApiError:
        pass

    # Missing table or incomplete extras: reprocess the resource
    logic.process_resource(resource, context, config)
    # Reload the resource to reflect updated extras
    refreshed, _ = _load_resource(resource["id"], for_update=False)
    return refreshed


def _flag_enabled(resource: Dict, flag_name: str) -> bool:
    extras = resource.get("extras") or []
    if isinstance(extras, dict):
        value = extras.get(flag_name)
    else:
        value = None
        for item in extras:
            if item.get("key") == flag_name:
                value = item.get("value")
                break
    return str(value).lower() in ("true", "1", "yes", "on")


def _drop_vector_table(resource: Dict) -> None:
    try:
        info = _vector_info(resource)
    except ShpToApiError:
        return
    try:
        db.drop_table(info.get("schema"), info["table"])
    except Exception:
        log.exception("Could not drop table %s", info.get("full_table"))

import logging
from typing import Dict

from ckan import model
from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IConfigurer, IResourceController, IBlueprint
from ckan.plugins import toolkit

from ckanext.shptoapi import log
from ckanext.shptoapi import logic, db
from ckanext.shptoapi.errors import ShpToApiError
from ckanext.shptoapi import routes


class ShpToApiPlugin(SingletonPlugin):
    implements(IConfigurer)
    implements(IResourceController, inherit=True)
    implements(IBlueprint)

    def update_config(self, config: Dict):
        config.setdefault("ckanext.shptoapi.enabled", False)
        config.setdefault("ckanext.shptoapi.auto_process", False)
        config.setdefault("ckanext.shptoapi.max_size_mb", 200)
        config.setdefault("ckanext.shptoapi.max_features", 50000)
        config.setdefault("ckanext.shptoapi.max_items", 1000)
        config.setdefault("ckanext.shptoapi.schema", "public")
        config.setdefault("ckanext.shptoapi.table_prefix", "vector_")
        toolkit.add_template_directory(config, "templates")

    def get_blueprint(self):
        return routes.create_blueprint(toolkit.config)

    def after_create(self, context, resource):
        self._process(resource, context)

    def after_update(self, context, resource):
        self._process(resource, context)

    def before_delete(self, context, resource, resources):
        try:
            extras = getattr(resource, "extras", {}) or {}
            if isinstance(extras, dict):
                vector_table = extras.get("vector_table")
                schema = extras.get("vector_schema")
            else:
                vector_table = None
                schema = None
                for extra in extras:
                    if extra.key == "vector_table":
                        vector_table = extra.value
                    if extra.key == "vector_schema":
                        schema = extra.value
            if vector_table:
                table = vector_table.split(".")[-1]
                db.drop_table(schema, table)
                log.info("Spatial table dropped: %s", vector_table)
        except Exception:
            log.exception("Could not drop associated spatial table.")

    def _process(self, resource, context):
        try:
            logic.process_resource(resource, context, toolkit.config)
        except ShpToApiError as exc:
            log.error("Error processing shapefile: %s", exc)
            raise toolkit.ValidationError(str(exc))
        except Exception as exc:
            log.exception("Unexpected error processing shapefile")
            raise toolkit.ValidationError(str(exc))

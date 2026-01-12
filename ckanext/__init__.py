try:
    import pkg_resources

    pkg_resources.declare_namespace(__name__)
except ImportError:  # pragma: no cover
    # If pkg_resources is not available we still want namespace behavior
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)

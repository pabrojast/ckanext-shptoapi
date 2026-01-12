from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    README = f.read()


setup(
    name="ckanext-shptoapi",
    version="0.1.0",
    description="Loads shapefiles into PostGIS and serves a Terria-compatible GeoJSON API.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="",
    license="MIT",
    packages=find_packages(),
    namespace_packages=["ckanext"],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "ckan.plugins": [
            "shptoapi=ckanext.shptoapi.plugin:ShpToApiPlugin",
        ],
    },
    install_requires=[
        "ckan>=2.9",
        "SQLAlchemy>=1.3,<2.0",
        "psycopg2-binary",
        "GDAL",
        "pyproj",
    ],
)

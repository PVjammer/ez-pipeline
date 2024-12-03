NAME = "ez_pipeline"
VERSION = "0.0.1"
DESCRIPTION = "Library for constructing data processing pipelines using the pipe '|' operator"
LONG_DESCRIPTION = "Coming Soon"
AUTHOR = "Nick Burnett"
AUTHOR_EMAIL = "PVjammer@gmail.com"
LICENSE = "MIT"

if __name__ == "__main__":
    from setuptools import setup
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        license=LICENSE,
        package_dir={'': 'src'},
        packages=["ez_pipeline"],
        python_requires='>=3.10',
    )
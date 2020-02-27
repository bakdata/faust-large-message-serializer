import pathlib

from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="faust-s3-backed-serializer",
    version="1.0.0",
    description="S3 Serializer for the Faust Framework",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/bakdata/faust-s3-backed-serializer",
    author="bakdata",
    author_email="opensource@bakdata.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.6",
    install_requires=[
        "faust >=1.8.0",
        "boto3"
    ],
    include_package_data=True,

)

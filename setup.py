from setuptools import setup, find_packages

# Read the version from version.py
with open("vepi/version.py", "r", encoding="utf-8") as f:
    __version__ = [line for line in f if line.startswith("__version__")][0].split('"')[1]
    
# Read the README file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="vena-etl-python-interface",
    version=__version__,
    author="Greg Hetherington", 
    author_email="ghetherington@venacorp.com",
    description="A Python library for interacting with Vena's ETL API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/venasolutions/vena-etl-python-interface", 
    packages=find_packages(),  # Automatically find all packages in the directory
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7", 
    install_requires=[
        "requests>=2.25.1",
        "pandas>=1.1.0",
    ],
    include_package_data=True,
)
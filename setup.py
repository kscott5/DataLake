# https://packaging.python.org/tutorials/packaging-projects/
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kscott5", # Replace with your own username
    version="0.0.1",
    author="Karega K Scott",
    author_email="kkscott@outlook.com",
    description="Simple datalake package-project", 
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kscott5/datalake",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPL-3.0-or-later", # https://spdx.org/licenses/
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
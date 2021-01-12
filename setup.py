import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bels", # Replace with your own username
    version="0.0.1",
    description="Biodiversity Enhanced Location Services",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Vertnet/bels",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

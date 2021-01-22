import setuptools
import sys

reqs = [
    'regex==2020.11.13',
    'chardet==4.0.0',
    'google-cloud-bigquery==2.6.1',
    'flask',
    'google-cloud-pubsub==2.2.0',
    'google-cloud-storage==1.35.0',
    'requests',
]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

sys.dont_write_bytecode = True
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
    install_requires=reqs,
)

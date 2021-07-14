import setuptools
import sys

reqs = [
    'regex==2021.4.4',
    'chardet==4.0.0',
    'google-cloud-bigquery==2.15.0',
    'flask',
    'google-cloud-pubsub==2.4.1',
    'google-cloud-storage==1.38.0',
    'requests',
    'sendgrid',
    'logging',
]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

sys.dont_write_bytecode = True
setuptools.setup(
    name="bels", # Replace with your own username
    version="0.0.2",
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

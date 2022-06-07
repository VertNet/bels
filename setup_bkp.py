# Do not run setup.py directly. See 
#    https://blog.ganssle.io/articles/2021/10/setup-py-deprecated.html
import setuptools
import sys

# reqs = [
#     'regex==2022.6.2',
#     'chardet==4.0.0',
#     'Flask',
#     'google-cloud-bigquery==3.1.0',
#     'google-cloud-pubsub==2.13.0',
#     'google-cloud-storage==2.3.0',
#     'requests==2.27.1',
#     'sendgrid==6.9.7',
#     'logging',
# ]

reqs = [
    'chardet',
    'Flask',
    'Flask-restful',
    'google-cloud-bigquery',
    'google-cloud-pubsub',
    'google-cloud-storage',
    'gunicorn',
    'logging',
    'regex',
    'requests',
    'sendgrid'
]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

sys.dont_write_bytecode = True
setuptools.setup(
    name="bels", # Replace with your own username
    version="0.0.3",
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

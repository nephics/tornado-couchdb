from setuptools import setup

setup(
    name='tornado-couchdb',
    version='0.2.1+',
    description="Blocking and non-blocking (asynchronous) clients for CouchDB using Tornado's httpclient",
    author='Jacob Sondergaard',
    author_email='jacob@nephics.com',
    license="MIT License",
    url='https://bitbucket.org/nephics/tornado-couchdb',
    packages=['couch'],
    requires=['tornado(>=3.1.1)'],
    classifiers=[
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3'
    ],
    download_url='https://bitbucket.org/nephics/tornado-couchdb/get/v0.2.1.tar.gz'
)
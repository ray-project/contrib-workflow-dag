from setuptools import setup, find_packages

version = {}
exec(open('contrib/version.py').read(), version)
VERSION = version['__version__']

setup(
    name='workflow-dag',
    version=VERSION,
    packages=find_packages(),
    author='Linsong Chu',
    author_email='lchu@us.ibm.com',
    description='workflow dag abstraction for ray workflow',
    install_requires=['ray']
)

import codecs
import os
import re
from distutils.core import setup


version = '0.0.0'  # stub for lint

with codecs.open(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'aioworkerpool', '__init__.py'), 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')


setup(
    name='aioworkerpool',
    version=version,
    packages=['aioworkerpool'],
    url='https://github.com/tumb1er/aioworkerpool',
    license='Beer license',
    author='Tumbler',
    author_email='zimbler@gmail.com',
    description='Asynchronous master/worker for asyncio',
    install_requires=[
        'python-daemon',
        'tblib',
    ]
)

#!/usr/bin/env python

from setuptools import setup, find_packages


def get_readme():
    with open('README.md', encoding="utf-8") as f:
        return f.read()

def get_requrements():
    with open('requirements.txt', encoding="utf-8") as f:
        return f.read()


setup(name='ndbc-api',
      version='0.1',
      description='Python National Data Buoy Center (NDBC) API.',
      long_description=get_readme(),
      classifiers=[
        'Programming Language :: Python :: 3.7',
      ],
      keywords='ndbc',
      url='http://github.com/cdjellen/pyndbc',
      author='Chris Jellen',
      author_email='cdjellen@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=["requests", "pandas"],
      entry_points={
          'console_scripts': ['pyndbc=pyndbc.pyndbc:main'],
      },
      include_package_data=True,
      zip_safe=False)

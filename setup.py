import setuptools

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='ndbc-api',
    packages=setuptools.find_packages(exclude=['*tests*']),
    version='0.24.1.6.1',
    license='MIT',
    description='A Python API for the National Data Buoy Center.',
    author='Chris Jellen',
    author_email='cdjellen@gmail.com',
    url='https://github.com/cdjellen/ndbc-api',
    long_description=long_description,
    long_description_content_type='text/markdown',
    download_url='https://github.com/cdjellen/ndbc-api/tarball/main',
    keywords=['ndbc', 'python3', 'api', 'oceanography', 'buoy', 'atmospheric'],
    install_requires=['requests', 'pandas', 'bs4', 'html5lib'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)

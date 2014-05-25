from setuptools import setup, find_packages

setup(
    name = 'ptutils',
    version = '0.0.3',
    author = 'Tao Peng',
    author_email = 'taopeng@meilishuo.com',
    packages = find_packages(),
    zip_safe = True,
    test_suite = 'nose.collector',
    tests_require = ['nose']
)

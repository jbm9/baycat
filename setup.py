from setuptools import setup

import unittest
def my_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    return test_suite


setup(name='baycat',
      version='0.1',
      description='A minimalist, efficiency-oriented S3 uploader',
      url='http://joshisanerd.com/projects/baycat/',
      author='Josh Myer',
      author_email='josh@joshisanerd.com',
      license='AGPL',
      packages=['baycat'],
      install_requires=[
          'boto3',
      ],
      test_suite="setup.my_test_suite",
      zip_safe=False)

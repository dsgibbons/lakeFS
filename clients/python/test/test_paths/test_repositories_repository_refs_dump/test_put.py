# coding: utf-8

"""


    Generated by: https://openapi-generator.tech
"""

import unittest
from unittest.mock import patch

import urllib3

import lakefs_client
from lakefs_client.paths.repositories_repository_refs_dump import put  # noqa: E501
from lakefs_client import configuration, schemas, api_client

from .. import ApiTestMixin


class TestRepositoriesRepositoryRefsDump(ApiTestMixin, unittest.TestCase):
    """
    RepositoriesRepositoryRefsDump unit test stubs
        Dump repository refs (tags, commits, branches) to object store  # noqa: E501
    """
    _configuration = configuration.Configuration()

    def setUp(self):
        used_api_client = api_client.ApiClient(configuration=self._configuration)
        self.api = put.ApiForput(api_client=used_api_client)  # noqa: E501

    def tearDown(self):
        pass

    response_status = 201




if __name__ == '__main__':
    unittest.main()
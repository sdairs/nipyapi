#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nipyapi` package."""

import pytest
from tests import conftest
import nipyapi


def test_list_schemas():
    # An empty Schema Registry response looks like:
    # [{'id': None, 'schema_metadata': None, 'timestamp': None}]
    r = nipyapi.schema.list_schemas()
    assert isinstance(r, list)
    assert r[0].attribute_map.__contains__('id')

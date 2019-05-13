#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nipyapi` package."""

import uuid
import pytest
from tests import conftest
from nipyapi import agents
from nipyapi.efm import FDConnection, FDRemoteProcessGroup, FDProcessor

# Tells pytest to skip this module of security testing is not enabled.
pytestmark = pytest.mark.skipif(not conftest.test_c2, reason='test_c2 disabled in Conftest')


def test_get_flow_designer_id_by_name():
    r1 = agents._get_flow_designer_id_by_name(conftest.test_minifijava_name)
    assert r1 is not None
    r2 = agents._get_flow_designer_id_by_name(conftest.test_minificpp_name)
    assert r2 is not None
    r3 = agents._get_flow_designer_id_by_name('nipyapi_test_minifi')  # deliberately short
    assert r3 is None


def test_get_flow_registry_id_by_name():
    r1 = agents._get_flow_registry_id_by_name(conftest.test_minifijava_name)
    assert r1 is not None
    r2 = agents._get_flow_designer_id_by_name('')  # deliberately short
    assert r2 is None
    r3 = agents._get_flow_designer_id_by_name('nipyapi_test_minifi')  # deliberately short
    assert r3 is None


def test_get_flow_publish_id_by_name():
    r1 = agents._get_flow_publish_id_by_name(conftest.test_minifijava_name)
    assert r1 is not None
    r2 = agents._get_flow_publish_id_by_name('')  # deliberately short
    assert r2 is None
    r3 = agents._get_flow_publish_id_by_name('nipyapi_test_minifi')  # deliberately short
    assert r3 is None


def test_get_root_pg_id():
    r1 = agents._get_root_pg_id(conftest.test_minificpp_name)
    assert r1 is not None
    r2 = agents._get_root_pg_id('')  # deliberately short
    assert r2 is None


def test_list_flow_components():
    r1 = agents._list_flow_components(conftest.test_minifijava_name)
    assert isinstance(r1, list)
    r2 = agents._list_flow_components('')  # deliberately short
    assert r2 is None


def test_create_connection():
    remote_port = str(uuid.uuid4())
    p1 = agents.create_processor('java-default', 'tailfile')
    rpg1 = agents.create_remote_pg('java-default', 'http://localhost:8080/nifi')
    c1 = agents.create_connection('java-default', p1, rpg1, remote_port=remote_port)
    assert isinstance(c1, FDConnection)
    p2 = agents.create_processor('java-default', 'tailfile')
    c2 = agents.create_connection('java-default', rpg1, p2, remote_port=remote_port)
    assert isinstance(c2, FDConnection)

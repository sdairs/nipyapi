#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nipyapi` package."""

import uuid
import pytest
from tests import conftest
from nipyapi import agents
from nipyapi.efm import FDConnection, FDRemoteProcessGroup, FDProcessor


def test_create_connection():
    remote_port = str(uuid.uuid4())
    p1 = agents.create_processor('java-default', 'tailfile')
    rpg1 = agents.create_remote_pg('java-default', 'http://localhost:8080/nifi')
    c1 = agents.create_connection('java-default', p1, rpg1, remote_port=remote_port)
    assert isinstance(c1, FDConnection)
    p2 = agents.create_processor('java-default', 'tailfile')
    c2 = agents.create_connection('java-default', rpg1, p2, remote_port=remote_port)

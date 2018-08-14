# -*- coding: utf-8 -*-

"""
For interactions with the Hortonworks Schema Registry.
"""

from __future__ import absolute_import
import logging
import nipyapi

__all__ = ['list_schemas']

log = logging.getLogger(__name__)


def list_schemas():
    """
    Returns a list of all Schemas in the Registry

    Returns:
        list[SchemaMetadataInfo]
    """
    return nipyapi.hwx_schema.SchemaApi().list_schemas()

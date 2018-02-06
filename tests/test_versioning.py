#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `nipyapi` package."""

from __future__ import absolute_import
import pytest
from tests import conftest
from nipyapi import registry, config, nifi, versioning, canvas, _utils


def test_create_registry_client():
    # First remove any leftover test client connections
    [versioning.delete_registry_client(li) for
     li in versioning.list_registry_clients().registries
     if conftest.test_registry_client_name in li.component.name
     ]
    r = versioning.create_registry_client(
        name=conftest.test_registry_client_name,
        uri=conftest.test_docker_registry_endpoint,
        description='a test connection'
    )
    assert isinstance(r, nifi.RegistryClientEntity)
    # test duplicate catch result
    with pytest.raises(ValueError):
        _ = versioning.create_registry_client(
            name=conftest.test_registry_client_name,
            uri=conftest.test_docker_registry_endpoint,
            description='who cares?'
        )


def test_list_registry_clients(fix_reg_client):
    r = versioning.list_registry_clients()
    assert isinstance(r, nifi.RegistryClientsEntity)


def test_delete_registry_client(fix_reg_client):
    r = versioning.delete_registry_client(fix_reg_client)
    assert isinstance(r, nifi.RegistryClientEntity)
    assert r.uri is None
    assert r.component.name == conftest.test_registry_client_name
    # TODO Add test for when a PG is attached to the client


def test_get_registry_client(fix_reg_client):
    r1 = versioning.get_registry_client(conftest.test_registry_client_name)
    assert isinstance(r1, nifi.RegistryClientEntity)
    assert r1.component.name == conftest.test_registry_client_name
    r2 = versioning.get_registry_client(r1.id, 'id')
    assert r2.id == r1.id


def test_list_registry_buckets(fix_bucket):
    r = versioning.list_registry_buckets()
    assert isinstance(r, list)
    assert len(r) >= 1


def test_create_registry_bucket(fix_reg_client):
    r = versioning.create_registry_bucket(conftest.test_bucket_name)
    assert isinstance(r, registry.Bucket)
    assert r.name == conftest.test_bucket_name
    # Bucket names are unique
    with pytest.raises(ValueError) as v:
        _ = versioning.create_registry_bucket(conftest.test_bucket_name)


def test_delete_registry_bucket(fix_bucket):
    r = versioning.delete_registry_bucket(fix_bucket.bucket)
    assert r.identifier == fix_bucket.bucket.identifier
    with pytest.raises(ValueError):
        _ = versioning.get_registry_bucket(
            fix_bucket.bucket.identifier,
            'id'
        )


def test_get_registry_bucket(fix_bucket):
    r1 = versioning.get_registry_bucket(conftest.test_bucket_name)
    assert r1.name == conftest.test_bucket_name
    r2 = versioning.get_registry_bucket(r1.identifier, 'id')
    assert r2.name == r1.name
    with pytest.raises(ValueError):
        _ = versioning.get_registry_bucket('Irrelevant', 'Invalid')
    r3 = versioning.get_registry_bucket('NonExistantProbably')
    assert r3 is None


def test_save_flow_ver(fix_bucket, fix_pg, fix_proc):
    f_pg = fix_pg.generate()
    r1 = versioning.save_flow_ver(
        process_group=f_pg,
        registry_client=fix_bucket.client,
        bucket=fix_bucket.bucket,
        flow_name=conftest.test_versioned_flow_name,
        comment='a test comment',
        desc='a test description'
    )
    assert isinstance(r1, nifi.VersionControlInformationEntity)
    # Next we validate you can't duplicate a flow name in a bucket
    with pytest.raises(ValueError):
        _ = versioning.save_flow_ver(
            process_group=f_pg,
            registry_client=fix_bucket.client,
            bucket=fix_bucket.bucket,
            flow_name=conftest.test_versioned_flow_name,
            comment='NiPyApi Test',
            desc='NiPyApi Test'
        )
    # Add a processor, refresh status, and save a new version
    fix_proc.generate(parent_pg=f_pg)
    f_pg = canvas.get_process_group(f_pg.id, 'id')
    r2 = versioning.save_flow_ver(
        process_group=f_pg,
        registry_client=fix_bucket.client,
        bucket=fix_bucket.bucket,
        flow_id=r1.version_control_information.flow_id,
        comment='a test comment'
    )
    assert isinstance(r2, nifi.VersionControlInformationEntity)
    assert r2.version_control_information.version > \
           r1.version_control_information.version


def test_stop_flow_ver(fix_ver_flow):
    r1 = versioning.stop_flow_ver(fix_ver_flow.pg)
    assert isinstance(r1, nifi.VersionControlInformationEntity)
    assert r1.version_control_information is None
    with pytest.raises(ValueError,
                       match='not currently under Version Control'):
        _ = versioning.stop_flow_ver(fix_ver_flow.pg)


def test_revert_flow_ver(fix_ver_flow):
    r1 = versioning.revert_flow_ver(fix_ver_flow.pg)
    assert isinstance(r1, nifi.VersionedFlowUpdateRequestEntity)
    # TODO: Add Tests for flows with data loss on reversion


def test_list_flows_in_bucket(fix_ver_flow):
    r1 = versioning.list_flows_in_bucket(fix_ver_flow.bucket.identifier)
    assert isinstance(r1, list)
    assert isinstance(r1[0], registry.VersionedFlow)
    with pytest.raises(ValueError, match='Bucket does not exist'):
        _ = versioning.list_flows_in_bucket('NiPyApi-FakeNews')


def test_get_flow_in_bucket(fix_ver_flow):
    r1 = versioning.get_flow_in_bucket(
        fix_ver_flow.bucket.identifier,
        fix_ver_flow.flow.identifier,
        'id'
    )
    assert isinstance(r1, registry.VersionedFlow)
    assert r1.identifier == fix_ver_flow.info.version_control_information.flow_id
    with pytest.raises(ValueError, match='Versioned flow does not exist'):
        _ = versioning.get_flow_in_bucket(
            fix_ver_flow.bucket.identifier,
            'fakenews',
            'id'
        )


def test_get_latest_flow_ver(fix_ver_flow):
    r1 = versioning.get_latest_flow_ver(
        fix_ver_flow.bucket.identifier,
        fix_ver_flow.flow.identifier
    )
    assert isinstance(r1, registry.VersionedFlowSnapshot)
    with pytest.raises(ValueError, match='Versioned flow does not exist'):
        _ = versioning.get_latest_flow_ver(
            fix_ver_flow.bucket.identifier,
            'fakenews'
        )


def test_update_flow_ver():
    # This function is more complicated than expected
    # Will implement in a future version
    pass


def test_get_version_info(fix_ver_flow):
    r1 = versioning.get_version_info(fix_ver_flow.pg)
    assert isinstance(r1, nifi.VersionControlInformationEntity)


def test_create_flow(fix_ver_flow):
    r1 = versioning.create_flow(
        bucket_id=fix_ver_flow.bucket.identifier,
        flow_name=conftest.test_cloned_ver_flow_name,
    )
    assert isinstance(r1, registry.VersionedFlow)
    assert r1.name == conftest.test_cloned_ver_flow_name
    # test duplicate behavior
    with pytest.raises(ValueError):
        _ = versioning.create_flow(
            bucket_id=fix_ver_flow.bucket.identifier,
            flow_name=conftest.test_cloned_ver_flow_name,
        )


def test_create_flow_version(fix_ver_flow):
    new_ver_stub = versioning.create_flow(
        bucket_id=fix_ver_flow.bucket.identifier,
        flow_name=conftest.test_cloned_ver_flow_name,
    )
    ver_flow_snapshot = versioning.get_latest_flow_ver(
        fix_ver_flow.bucket.identifier,
        fix_ver_flow.flow.identifier
    )
    r1 = versioning.create_flow_version(
        flow=new_ver_stub,
        flow_snapshot=ver_flow_snapshot
    )
    assert isinstance(r1, registry.VersionedFlowSnapshot)
    # registry bug https://issues.apache.org/jira/browse/NIFIREG-135
    # assert r1.flow.version_count == 2


def test_get_flow_version(fix_ver_flow):
    r1 = versioning.get_flow_version(
        fix_ver_flow.bucket.identifier,
        fix_ver_flow.flow.identifier
    )
    assert isinstance(r1, registry.VersionedFlowSnapshot)
    assert r1.snapshot_metadata.version == 1
    test_vf_2 = versioning.create_flow_version(
        bucket_id=fix_ver_flow.bucket.identifier,
        flow=r1.flow,
        flow_snapshot=r1
    )
    assert isinstance(test_vf_2, registry.VersionedFlowSnapshot)
    assert test_vf_2.snapshot_metadata.version == 2
    r2 = versioning.get_flow_version(
        fix_ver_flow.bucket.identifier,
        test_vf_2.snapshot_metadata.flow_identifier
    )
    assert r2.flow.version_count == 2
    assert r2.snapshot_metadata.version ==2
    r3 = versioning.get_flow_version(
        fix_ver_flow.bucket.identifier,
        test_vf_2.snapshot_metadata.flow_identifier,
        version='1'
    )
    assert r3.snapshot_metadata.version == 1
    assert r3.flow.version_count == 2


def test_export_flow(fix_flow_serde):
    # Test we can turn a flow snapshot into a json string
    r1 = versioning.export_flow(fix_flow_serde.snapshot)
    assert isinstance(r1, str)
    # Test writing it to a file
    r2 = versioning.export_flow(
        flow_snapshot=fix_flow_serde.snapshot,
        file_path=fix_flow_serde.filepath,
        mode='json'
    )
    assert isinstance(r2, str)


def test_import_flow(fix_flow_serde):
    # Test we can import from a String in memory
    # Test we can import as new version in existing bucket
    r1 = versioning.import_flow(
        bucket_id=fix_flow_serde.bucket.identifier,
        encoded_flow=fix_flow_serde.json,
        flow_id=fix_flow_serde.flow.identifier
    )
    assert isinstance(r1, registry.VersionedFlowSnapshot)

    # Test we can also import from a file
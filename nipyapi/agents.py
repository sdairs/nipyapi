# -*- coding: utf-8 -*-

"""
MiNiFi Agent management via EFM for NiPyApi

Warnings: Experimental
"""

from __future__ import absolute_import
import six
import logging
import nipyapi

log = logging.getLogger(__name__)
__ac_api = nipyapi.efm.AgentClassesApi()
__am_api = nipyapi.efm.AgentManifestsApi()
__fd_api = nipyapi.efm.FlowDesignerApi()
__f_api = nipyapi.efm.FlowsApi()


def create_processor(flow_name, type_name, pg_id=None, position=None, name=None,
                     properties=None, schedule=None, terminate=None, concurrency=None):
    # Handle defaults
    position = position if position else suggest_object_position(flow_name)
    pg_id = pg_id if pg_id else _get_root_pg_id(flow_name)
    # First, fetch the basic processor definition from the Flow associated with this agent class
    proc_summary = [
        x for x in list_processor_types(flow_name)
        if type_name.lower() in x.type.lower()
    ]
    if not proc_summary:
        raise ValueError("Processor Type %s not found", type_name)
    if len(proc_summary) != 1:
        raise ValueError("More than one Processor Type [%s] found containing %s",
                         str([x.type for x in proc_summary]), type_name)
    proc_summary = proc_summary[0]
    # Second create it
    return __fd_api.create_processor(
        flow_id=_get_flow_designer_id_by_name(flow_name),
        pg_id=pg_id,
        body=nipyapi.efm.FDProcessor(
            revision={'version': 0},
            component_configuration=nipyapi.efm.VersionedProcessor(
                bundle=nipyapi.efm.Bundle(
                    artifact=proc_summary.artifact,
                    group=proc_summary.group,
                    version=proc_summary.version
                ),
                type=proc_summary.type,
                position=position,
                name=name,
                properties=properties,
                scheduling_period=schedule,
                auto_terminated_relationships=terminate,
                concurrently_schedulable_task_count=concurrency
            )
        )
    )


def create_connection(flow_name, source, dest, relationships=None, remote_port=None, name=None):
    # Handle defaults
    relationships = relationships if relationships else ["success"]
    # prepare submission objects
    if 'RemoteProcessGroup' in str(type(source)):
        relationships = None
        assert remote_port is not None, "remote_port must be set to connect a remote process group"
        if 'Versioned' not in str(type(source)):
            source_sub = nipyapi.efm.ConnectableComponent(
                group_id=source.component_configuration.identifier,
                id=remote_port,
                type='REMOTE_OUTPUT_PORT'
            )
        else:
            source_sub = nipyapi.efm.ConnectableComponent(
                group_id=source.identifier,
                id=remote_port,
                type='REMOTE_OUTPUT_PORT'
            )
    else:
        # handle regular source
        if 'Versioned' not in str(type(dest)):
            source_sub = nipyapi.efm.ConnectableComponent(
                group_id=source.component_configuration.group_identifier,
                id=source.component_configuration.identifier,
                type=source.component_configuration.component_type
            )
        else:
            source_sub = nipyapi.efm.ConnectableComponent(
                group_id=source.group_identifier,
                id=source.identifier,
                type=source.component_type
            )
    if 'RemoteProcessGroup' in str(type(dest)):
        assert remote_port is not None, "remote_port must be set to connect a remote process group"
        if 'component_configuration' in source.attribute_map:
            dest_sub = nipyapi.efm.ConnectableComponent(
                group_id=dest.component_configuration.identifier,
                id=remote_port,
                type='REMOTE_INPUT_PORT'
            )
        else:
            dest_sub = nipyapi.efm.ConnectableComponent(
                group_id=dest.identifier,
                id=remote_port,
                type='REMOTE_INPUT_PORT'
            )
    else:
        # handle regular destination
        dest_sub = nipyapi.efm.ConnectableComponent(
            group_id=dest.component_configuration.group_identifier,
            id=dest.component_configuration.identifier,
            type=dest.component_configuration.component_type
        )
    # Create it
    return __fd_api.create_connection(
        flow_id=_get_flow_designer_id_by_name(flow_name),
        pg_id=_get_root_pg_id(flow_name),
        body=nipyapi.efm.FDConnection(
            revision={'version': 0},
            component_configuration=nipyapi.efm.VersionedConnection(
                source=source_sub,
                selected_relationships=relationships,
                destination=dest_sub,
                name=name
            )
        )
    )


def create_remote_process_group(flow_name, target_uris, pg_id=None, position=None, protocol=None):
    position = position if position else suggest_object_position(flow_name)
    pg_id = pg_id if pg_id else _get_root_pg_id(flow_name)
    return __fd_api.create_remote_process_group(
        flow_id=_get_flow_designer_id_by_name(flow_name),
        pg_id=pg_id,
        body=nipyapi.efm.FDRemoteProcessGroup(
            revision={'version': 0},
            component_configuration=nipyapi.efm.VersionedRemoteProcessGroup(
                position=position,
                target_uris=target_uris,
                transport_protocol=protocol
            )
        )
    )

#######################


def list_processor_types(flow_name):
    di = _get_flow_designer_id_by_name(flow_name)
    return __fd_api.get_processor_types(di).component_types


def list_controller_types(flow_name):
    di = _get_flow_designer_id_by_name(flow_name)
    return __fd_api.get_controller_service_types(di).component_types


def purge_canvas(flow_name):
    _remove_flow_components(flow_name, _list_flow_components(flow_name))


def suggest_object_position(flow_name):
    # snakes down then up from left to right with noted spacing and row count
    obj_spacing = 200
    obj_per_col = 5
    flow_components = _list_flow_components(flow_name)
    if not flow_components:
        return nipyapi.efm.Position(obj_spacing,  obj_spacing)
    current_positions = [i.position for i in flow_components]
    object_count = len(current_positions)
    max_x = max([
        i.x for i in current_positions
    ])
    y_set = [
        i.y for i in current_positions
        if i.x == max_x
    ]
    if object_count % obj_per_col == 0:
        # changing direction
        out_x = max_x + obj_spacing
        freeze_y = True
    else:
        # going down or up, x stays same
        out_x = max_x
        freeze_y = False
    if int(object_count / obj_per_col) % 2 == 0:
        # going down
        if freeze_y:
            out_y = min(y_set)
        else:
            out_y = min(y_set) + obj_spacing
    else:
        # going up
        if freeze_y:
            out_y = max(y_set)
        else:
            out_y = min(y_set) - obj_spacing
    return nipyapi.efm.Position(x=out_x, y=out_y)

#######################


def list_flow_names():
    # currently a 1:1 mapping of flow names to agent class names
    return [
        x.name for x in __ac_api.get_agent_classes()
    ]

#######################


def export_published_flow(flow_name, yaml=True, filename=None):
    assert isinstance(yaml, bool)
    pi = _get_flow_publish_id_by_name(flow_name)
    if not pi:
        return False
    if yaml:
        out = __f_api.get_flow_content_as_yaml(pi)
        if filename:
            nipyapi.utils.fs_write(out, filename)
            return filename
        else:
            return out
    else:
        return __f_api.get_flow(pi)


def publish_canvas_flow(flow_name):
    __fd_api.publish_flow(_get_flow_designer_id_by_name(flow_name))


def revert_canvas_flow(flow_name):
    __fd_api.revert_flow(_get_flow_designer_id_by_name(flow_name))


def import_flow_to_canvas(flow_name, filename=None, yaml=True, overwrite=True):
    if overwrite:
        purge_canvas(flow_name)
    if not yaml or not filename:
        raise ValueError("We currently only support importing yaml from a file")
    # Read in Flow Def
    flow_def = nipyapi.utils.load(nipyapi.utils.fs_read(filename))
    # Validate Flow Def
    def_ver = flow_def.pop('NiPyAPI Agent Config Version')
    assert def_ver == 1, "Flow Definition version is bad"
    unique_names = set([i['name'] for j in [flow_def[x] for x in flow_def.keys()] for i in j])
    assert len(unique_names) == len(flow_def.keys()), "All Component names must be unique"
    if 'Processors' in flow_def:
        for proc in flow_def['Processors']:
            create_processor(
                flow_name=flow_name,
                name=proc['name'],
                type_name=proc['class'],
                schedule=proc['scheduling period'],
                properties=proc['Properties'],
                concurrency=proc['concurrency'] if 'concurrency' in proc else None
            )
    if 'Remote Process Groups' in flow_def:
        for rpg in flow_def['Remote Process Groups']:
            create_remote_process_group(
                flow_name=flow_name,
                target_uris=rpg['name'],
                protocol=rpg['transport_protocol']
            )
    if 'Connections' in flow_def:
        for con in flow_def['Connections']:
            source = _get_flow_component_by_name(flow_name, con['source'])
            dest = _get_flow_component_by_name(flow_name, con['destination'])
            assert source is not None
            assert dest is not None
            # Lookup remote port id in NiFi if specified
            if 'port' in con.keys():
                nifi_port = [
                    x for x in
                    nipyapi.canvas.list_all_input_ports() + nipyapi.canvas.list_all_output_ports()
                    if x.status.name == con['port']
                ]
                if not nifi_port or len(nifi_port) > 1:
                    raise ValueError("Remote Process Group Port [%s] not found or name not unique", con['port'])
                remote_port = nifi_port[0].id
            else:
                remote_port = None
            create_connection(
                flow_name=flow_name,
                source=source,
                dest=dest,
                relationships=con['source relationship names'],
                remote_port=remote_port,
                name=con['name']
            )

    return filename

#######################


def _get_flow_designer_id_by_name(flow_name):
    out = [
        x['identifier'] for x in __fd_api.get_flows().elements
        if x['agentClass'] == flow_name  # Note exact match requirement
    ]
    if out:  # flow_name == '' may produce an empty list
        return out[0]  # Flow Names are unique so we enforce unique lookup
    return None


def _get_flow_registry_id_by_name(flow_name):
    di = _get_flow_designer_id_by_name(flow_name)
    if di is not None:
        out = __fd_api.get_flow_version_info(di)
        if out is not None:
            return out.version_info.registry_flow_id
    return None


def _get_flow_publish_id_by_name(flow_name):
    ri = _get_flow_registry_id_by_name(flow_name)
    if ri is not None:
        flow_vers = [
            x for x in __f_api.get_all_flow_summaries().flows
            if ri == x.registry_flow_id
        ]
        if flow_vers is not None:
            sorted_flow_vers = sorted(flow_vers, key=lambda x: x.registry_flow_version)
            return sorted_flow_vers[-1].id
    return None


def _get_root_pg_id(flow_name):
    out = [
        x['rootProcessGroupIdentifier'] for x in
        __fd_api.get_flows().elements
        if x['agentClass'] == flow_name  # exact match
    ]
    if out:
        return out[0]  # Flow names are unique
    return None


def _list_flow_components(flow_name):
    positionable_objects = ['processors', 'funnels', 'remote_process_groups']
    di = _get_flow_designer_id_by_name(flow_name)
    if di is not None:
        flow = __fd_api.get_flow(di)
        return [
            i for s in
            [flow.flow_content.__getattribute__(x) for x in positionable_objects]
            for i in s
        ]
    return []


def _remove_flow_components(flow_name, components):
    fi = _get_flow_designer_id_by_name(flow_name)
    if fi is not None:
        return_list = []
        for component in components:
            get_handle = __fd_api.__getattribute__('get_' + component.component_type.lower())
            target_obj = get_handle(fi, component.identifier)
            del_handle = __fd_api.__getattribute__('delete_' + component.component_type.lower())
            return_list += del_handle(
                fi,
                target_obj.component_configuration.identifier,
                version=target_obj.revision.version
            )
        return return_list
    return False


def _get_flow_component_by_name(flow_name, component_name):
    flow_components = _list_flow_components(flow_name)
    if flow_components:
        out = [
            x for x in _list_flow_components(flow_name)
            if component_name == x.name
        ]
        if out is not None:
            if len(out) > 1:
                raise ValueError("Name not unique on Canvas")
            return out[0]
    return None

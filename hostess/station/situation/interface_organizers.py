"""
functions used by the situation app to retrieve and organize the content
of a Station's situation dump.

functions in this module should not interact with `textual`.
"""
import datetime as dt
import time
from typing import Mapping

from cytoolz import keyfilter, keymap, valmap
from dustgoggles.structures import NestingDict

from hostess.station.comm import read_comm
from hostess.station.messages import unpack_obj
from hostess.station.proto import station_pb2 as pro
from hostess.station.talkie import stsend


def get_situation(host, port):
    if (host is None) or (port is None):
        raise TypeError("station address not defined")
    # TODO: handle recurring queries a little more gracefully
    response, _ = stsend(b"situation", host, port, timeout=0.5)
    # TODO: timeout tracker
    if response == "timeout":
        raise TimeoutError('dropped packet')
    if response == 'connection refused':
        raise ConnectionError('connection refused')
    if response == b'shutting down':
        raise ConnectionError('station in shutdown mode')
    try:
        comm = read_comm(response)
    except TypeError:
        raise ValueError(f'bad response: {response[:128]}')
    if not isinstance(comm['body'], pro.PythonObject):
        raise ValueError(f'bad response: {comm["body"][:128]}')
    return unpack_obj(comm["body"])


def add_config_to_elements(elements: dict, config: dict) -> dict[str]:
    """join config information to matching Actors/Sensors"""
    out = {}
    for name, classname in elements.items():
        out[name] = {'class': classname}
        element_interface = keyfilter(
            lambda k: k.split('_')[0] == name, config['interface']
        )
        if len(element_interface) > 0:
            out[name]['interface'] = keymap(
                lambda k: "_".join(k.split("_")[1:]), element_interface
            )
        if (element_cdict := config.get('cdict', {}).get(name)) is not None:
            out[name]['cdict'] = element_cdict
    return out


def organize_tasks(tasks: dict) -> dict:
    out = NestingDict()
    for code, task in tasks.items():
        times = valmap(
            lambda t: t if not isinstance(t, dt.datetime) else t.isoformat(),
            keyfilter(lambda f: f.endswith("_time"), task)
        )
        title = f"{times['init_time'][:21]} ({code})"
        target = out[task['status']][task['name']][title]
        target['times'] = times
        target['delegate'] = task['delegate']
        for k in ('description', 'duration'):
            if (v := task.get(k)) not in (None, {}):
                target[k] = v
        if target.get('duration') is None:
            target['duration'] = time.time() - task['init_time'].timestamp()
        # NOTE: skipping action id as it is currently nowhere used.
    for k in ('success', 'running', 'sent', 'pending', 'crash'):
        # noinspection PyStatementEffect
        out[k]
    return out
    # return out.todict()


def organize_running_actions(reports: dict) -> dict:
    out = NestingDict()
    for r in reports:
        times = r['time']
        title = f"{times['start'][:21]} ({r['instruction_id']})"
        target = out[r['name']][title]
        target['times'] = times
        # TODO: maybe we should squeeze descriptions into the ActionReports?
        target['duration'] = (
             time.time()
             - dt.datetime.fromisoformat(times['start']).timestamp()
        )
    return out.todict()


def delegate_dict(ddict: Mapping) -> dict:
    config = {
        'cdict': ddict.get('cdict', {}),
        'interface': ddict.get('interface', {})
    }
    out = {'status': ddict['inferred_status'], 'wait_time': ddict['wait_time']}
    if ddict.get('busy') is True:
        ddict['wait_time'] = str(out['wait_time']) + ' [busy]'
    for element_type in ('actors', 'sensors'):
        if len(element_dict := ddict.get(element_type, {})) > 0:
            out[element_type] = add_config_to_elements(element_dict, config)
    if len(ddict.get('running', [])) > 0:
        out['running'] = organize_running_actions(ddict['running'])
    else:
        out['running'] = []
    return out


def organize_delegates(view: dict) -> dict:
    out = {}
    for name, ddict in view['delegates'].items():
        title = (
            f"{name}@{ddict.get('host', '?')}: (PID {ddict.get('pid', '?')})"
        )
        out[title] = delegate_dict(ddict)
    return out


def organize_station(view: dict) -> dict:
    view = keyfilter(lambda k: k != 'delegates', view)
    return {
        'id': f"{view['name']}@{view.get('host', '?')}:{view['port']}",
        'actors': add_config_to_elements(view['actors'], view['config']),
        'tasks': organize_tasks(view['tasks']),
        'threads': view['threads']
    }

# coding: utf-8
import os

import packtrack
import requests

from database import MongoDb as Database


def correios(track, backend=None):
    if backend is None:
        backend = os.getenv('ECT_BACKEND')
    encomenda = packtrack.Correios.track(track, backend=backend)
    if not encomenda:
        raise ValueError(u"Encomenda nao encontrada.")
    if not encomenda.status:
        raise ValueError(u"A encomenda ainda nao tem historico.")

    result = []
    for status in encomenda.status:
        historico = {
            'data': status.data,
            'local': status.local,
            'situacao': status.situacao,
            'detalhes': status.detalhes,
        }
        result.append(historico)
    return result


def register(provider, track, callback):
    """
    Registra o pacote para acompanhamento.
    """
    db = Database()
    return db.packtrack.register_callback(provider, track, callback)


def run(provider, track):
    db = Database()
    obj = db.packtrack.get_one(provider, track)

    if provider != 'ect':
        raise ValueError(u"Unexpected provider: %s" % provider)

    try:
        data = correios(track)
    except ValueError:
        return False
    changed = obj.get('payload') != data
    db.packtrack.update_response(provider, track, data, changed=changed)
    return changed


def report(provider, track):
    db = Database()
    obj = db.packtrack.get_one(provider, track)
    obj['token'] = str(obj.pop('_id'))

    _meta = obj['_meta']
    callbacks = _meta.pop('callbacks')

    for cb in callbacks:
        callback = cb.pop('callback')
        data = dict(obj, **cb)
        headers = {'Content-Type': 'application/json'}
        requests.post(callback, data=data, headers=headers)

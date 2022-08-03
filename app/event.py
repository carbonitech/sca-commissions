from typing import Dict, Hashable

subscribers: Dict[Hashable,set] = dict()

def subscribe(event_type: Hashable, function_call):
    if event_type not in subscribers:
        subscribers[event_type] = set()
    subscribers[event_type].add(function_call)

def post_event(event_type: Hashable, data, *args, **kwargs):
    if event_type not in subscribers:
        return
    for func in subscribers[event_type]:
        func(data, *args, **kwargs)
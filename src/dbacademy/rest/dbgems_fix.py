"""
Hack to work around module import problems.
"""

__all__=["dbgems"]


def _load_dbgems():
    import dbacademy
    if hasattr(dbacademy, "dbgems"):
        return getattr(dbacademy, "dbgems")
    try:
        import dbacademy.dbgems
        return dbacademy.dbgems
    except ModuleNotFoundError:
        pass
    import sys
    from os.path import exists
    from importlib.util import spec_from_file_location, module_from_spec
    module_name = "dbacademy.dbgems"
    rel_path = "dbacademy/dbgems/__init__.py"
    for path in sys.path:
        path = path + "/" + rel_path
        if exists(path):
            break
    else:
        raise ModuleNotFoundError("dbacademy.dbgems not found")
    spec = spec_from_file_location(module_name, path)
    module = module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    dbacademy.dbgems = module
    return module


dbgems = _load_dbgems()

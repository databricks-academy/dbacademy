def validate_dependencies():
    try:
        assert validate_dependencies_already
    except NameError:
        from dbacademy import dbgems
        dbgems.validate_dependencies("dbacademy")
        validate_dependencies_already = True


validate_dependencies()

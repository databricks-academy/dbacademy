def validate_dependencies():
    try:
        # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
        assert validated_dependencies
    except NameError:
        from dbacademy import dbgems
        dbgems.validate_dependencies("dbacademy")
        validated_dependencies = True


validate_dependencies()

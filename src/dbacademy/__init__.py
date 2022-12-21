def validate_dependencies():
    try:
        # noinspection PyUnresolvedReferences,PyUnboundLocalVariable
        assert validated_dependencies
    except NameError:
        try:
            # noinspection PyUnusedLocal
            validated_dependencies = True
            from dbacademy import dbgems
            dbgems.validate_dependencies("dbacademy")
        except:
            pass


validate_dependencies()

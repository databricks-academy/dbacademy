class Stub:
    def __init__(self):
        pass

    # noinspection PyPep8Naming
    @staticmethod
    def mapAsJavaMap(tags):
        return tags


class MockSparkContext:
    def __init__(self):

        self._jvm = Stub()
        self._jvm.scala = Stub()
        self._jvm.scala.collection = Stub()
        self._jvm.scala.collection.JavaConversions = Stub()

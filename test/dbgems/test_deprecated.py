import unittest

def deprecation_logging_enabled():
    return True
    # status = spark.conf.get("dbacademy.deprecation.logging", None)
    # return status is not None and status.lower() == "enabled"

def print_warning(title: str, message: str, length: int = 80, include_trace=False):
    title_len = length - len(title) - 3
    print(f"""* {title.upper()} {("*"*title_len)}""")
    for line in message.split("\n"):
        print(f"* {line}")
    print("*"*length)

def deprecated(reason=None):
    def decorator(inner_function):
        def wrapper(*args, **kwargs):
            if deprecation_logging_enabled():
                assert reason is not None, f"The deprecated reason must be specified."
                try:
                    import inspect
                    function_name = str(inner_function.__name__) + str(inspect.signature(inner_function))
                    final_reason = f"{reason}\n{function_name}"
                except:
                    # Just in case it chokes for some reason
                    final_reason = reason
                print_warning(title="DEPRECATED", message=final_reason, include_trace=True)
            result = inner_function(*args, **kwargs)
            return result
        return wrapper
    return decorator

@deprecated(reason="moo")
def add(a, b):
    return a+b

class MyTestCase(unittest.TestCase):

    def test_something(self):
        result = add(1, 3)
        self.assertEqual(result, 3)  # add assertion here


if __name__ == '__main__':
    unittest.main()

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    """
    Decorator that makes a property lazy-evaluated.
    """
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazy_property


_TEST_RESULTS_STYLE = """
    <style>
      table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
      caption { text-align: left; padding: 5px }
      th, td { border: 1px solid #ddd; padding: 5px }
      th { background-color: #ddd }
      .passed { background-color: #97d897 }
      .failed { background-color: #e2716c }
      .skipped { background-color: #f9d275 }
      .results .points { display: none }
      .results .message { display: block; font-size:smaller; color:gray }
      .results .note { display: block; font-size:smaller; font-decoration:italics }
      .results .passed::before  { content: "Passed" }
      .results .failed::before  { content: "Failed" }
      .results .skipped::before { content: "Skipped" }
      .grade .passed  .message:empty::before { content:"Passed" }
      .grade .failed  .message:empty::before { content:"Failed" }
      .grade .skipped .message:empty::before { content:"Skipped" }
    </style>
        """.strip()

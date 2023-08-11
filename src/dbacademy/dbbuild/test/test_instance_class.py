class TestInstance:
    def __init__(self, build_config, notebook, test_dir, test_type):
        import hashlib

        self.notebook = notebook
        self.job_id = 0
        self.run_id = 0
        self.test_type = test_type

        if notebook.include_solution:
            self.notebook_path = f"{test_dir}/Solutions/{notebook.path}"
        else:
            self.notebook_path = f"{test_dir}/{notebook.path}"

        hash_code = str(hashlib.sha256(self.notebook_path.encode()).hexdigest())[:6]
        test_name = build_config.name.lower().replace(" ", "-")
        self.job_name = f"[TEST] {test_name} | {test_type} | {hash_code}"

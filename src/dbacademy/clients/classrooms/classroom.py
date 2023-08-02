from dbacademy.clients.dougrest import DatabricksApi


class Classroom(object):
    import re

    def __init__(self, num_students, class_number=None, username_pattern=None,
                 hostname=None, access_token=None, databricks_api=None, spark_version="11.3.x-cpu-ml-scala2.12"):
        """
        Create a Classroom control object.

        >>> classroom=Classroom(num_students=50, class_number=1360)
        """
        self.databricks = databricks_api or DatabricksApi(hostname=hostname, token=access_token)
        self.num_students = num_students
        if not class_number and not username_pattern:
            raise Exception("Either the class_number or username_pattern must be specified.")
        self.username_pattern = (username_pattern or
                                 "class-{class_number}-{student_number}@azuredatabrickstraining.onmicrosoft.com".format(
                                     class_number=class_number, student_number="{student_number:03d}"))
        self.pool_name = "Class Pool"
        self.timeout_minutes = 120
        self.default_spark_version = spark_version

    def start_clusters(self, last_student=None, machine_type=None, num_workers=2, first_student=0):
        """
        Launch the student clusters.

        >>> classroom = Classroom()
        >>> classroom.start_clusters()
        """
        if last_student is None:
            last_student = self.num_students
        #     instance_pool_id = self.databricks.pools.edit_or_create(
        #       name=self.pool_name,
        #       machine_type=machine_type,
        #       min_idle=3)
        #     self.databricks.pools.add_to_acl(instance_pool_id, group_permissions={"users": "CAN_ATTACH_TO"})
        instance_pool_id = None
        clusters = self.databricks.clusters.list()
        for i in range(first_student, last_student + 1):
            cluster_name = f"cluster-{i:03d}"
            user_name = self.username_pattern.format(student_number=i)
            cluster_id = self.databricks.clusters.create_or_start(
                name=cluster_name,
                machine_type=machine_type,
                timeout_minutes=self.timeout_minutes,
                instance_pool_id=instance_pool_id,
                num_workers=num_workers,
                #         num_cores="8" if num_workers==0 else "*",
                existing_clusters=clusters)
            self.databricks.clusters.set_acl(cluster_id, {user_name: "CAN_MANAGE"})

    def terminate_clusters(self):
        """
        Terminate all running clusters.

        >>> classroom = Classroom()
        >>> classroom.terminate_clusters()
        """
        self.databricks.pools.edit_by_name(self.pool_name, min_idle=0)
        clusters = self.databricks.clusters.list()
        my_cluster_id = self.databricks.cluster_id
        cluster_ids = (c["cluster_id"] for c in clusters if c["cluster_id"] != my_cluster_id)
        for c in cluster_ids:
            self.databricks.clusters.terminate(c)

    #     self.databricks.clusters.terminate(my_cluster_id)

    def delete_clusters(self):
        """
        Terminate all running clusters.

        >>> classroom = Classroom()
        >>> classroom.terminate_clusters()
        """
        self.databricks.pools.edit_by_name(self.pool_name, min_idle=0)
        clusters = self.databricks.clusters.list()
        my_cluster_id = self.databricks.cluster_id
        cluster_ids = (c["cluster_id"] for c in clusters if c["cluster_id"] != my_cluster_id)
        for c in cluster_ids:
            self.databricks.clusters.delete(c)

    #     self.databricks.clusters.delete(my_cluster_id)

    def delete_folder(self, folder_name, last_student=None, first_student=0):
        """
        Remove folder from student directories.
        This is typically used to delete old courseware prior to uploading a new version.

        >>> classroom = Classroom()
        >>> classroom.delete_folder("Spark-ILT")
        """
        if last_student is None:
            last_student = self.num_students
        for i in range(first_student, last_student + 1):
            user_name = self.username_pattern.format(student_number=i)
            folder_path = f"/Users/{user_name}/{folder_name}"
            self.databricks.workspace.delete(folder_path)

    def upload_dbc(self, source_url, folder_name=None, last_student=None, first_student=0):
        """
        Upload courseware.

        >>> classroom = Classroom()
        >>> classroom.upload_dbc("https://files.training.databricks.com/courses/spark-ilt/Lessons.dbc", \
        ... "Spark-ILT")
        """
        if folder_name is None:
            folder_name = self.extract_filename(source_url)
        if last_student is None:
            last_student = self.num_students
        content = self.databricks.workspace.read_data_from_url(source_url, format="DBC")
        for i in range(first_student, last_student + 1):
            user_name = self.username_pattern.format(student_number=i)
            folder_path = f"/Users/{user_name}/{folder_name}"
            self.databricks.workspace.import_from_data(content, folder_path, format="DBC", if_exists="ignore")

    def create_users(self, last_student=None, first_student=0, allow_cluster_create=False):
        """
        Add users class+000@databricks.com to class+050@databricks.com (or whatever limit is set by last_student).
        User class+000@databricks.com receives admin rights, the others do not.

        >>> classroom = Classroom()
        >>> classroom.create_users()
        """
        if last_student is None:
            last_student = self.num_students

        existing_users = self.databricks.users.list_usernames()

        for i in range(first_student, last_student + 1):
            user_name = self.username_pattern.format(student_number=i)
            if user_name in existing_users:
                continue
            self.databricks.users.create(user_name, allow_cluster_create)
            if i == 0:  # User 0 gets admin rights.
                self.databricks.groups.add_member("admins", user_name=user_name)

    invalid_char_regex = re.compile(r'\W')
    dbc_regex = re.compile(r'^.*/([^/]*).dbc$')

    @classmethod
    def extract_filename(cls, url):
        match = cls.dbc_regex.search(url)
        if not match:
            raise Exception(f'Unable to extract filename from {url}, please provide it manually.')
        return cls.invalid_char_regex.sub('-', match[1])

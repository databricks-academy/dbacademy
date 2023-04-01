import os
from dbacademy.workspaces_3_0.account_config_class import AccountConfig
from dbacademy.workspaces_3_0.uc_storage_config_class import UcStorageConfig
from dbacademy.workspaces_3_0.workspace_config_classe import WorkspaceConfig
from dbacademy.workspaces_3_0.workspace_setup_class import WorkspaceSetup

total_workspaces = 10
first_workspace_number = 400
max_participants = 250

uc_storage_config = UcStorageConfig(storage_root="s3://unity-catalogs-us-west-2/",
                                    storage_root_credential_id="be2549d1-3f5b-40db-900d-1b0fcdb419ee",
                                    region="us-west-2",
                                    meta_store_owner="instructors",
                                    aws_iam_role_arn=None,         # One of these two values is required
                                    msa_access_connector_id=None)  # The other should be set to None

workspace_config_template = WorkspaceConfig(max_participants=max_participants,
                                            datasets=None,  # Appended to based on course_definitions; Only needs to be defined for DAWD
                                            course_definitions=[
                                                # "course=welcome",
                                                # "course=example-course&version=v1.1.8",
                                                # "course=template-course&version=v1.0.0&artifact=template-course.dbc",
                                                # "https://labs.training.databricks.com/api/v1/courses/download.dbc?course=ml-in-production"
                                            ],
                                            cds_api_token=os.environ.get("WORKSPACE_SETUP_CDS_API_TOKEN"),
                                            default_dbr="11.3.x-cpu-ml-scala2.12",
                                            default_node_type_id="i3.xlarge",
                                            credentials_name="default",
                                            storage_configuration="us-west-2",                         # Not region, just named after the region.
                                            username_pattern="class+{student_number}@databricks.com",  # student_number defaults to 3 digits, zero prefixed
                                            entitlements={  # TODO - this should be applied to the "users" group, not each user.
                                                "allow-cluster-create": False,        # False to enforce policy
                                                "allow-instance-pool-create": False,  # False to enforce policy
                                                "databricks-sql-access": True,
                                                "workspace-access": True,
                                            },
                                            # Cloud Labs needs are different here because they have a list of users, not a range of users.
                                            workspace_name_pattern="classroom-{workspace_number}",  # workspace_number defaults to 3 digits, zero prefixed
                                            workspace_group={                                       # User class+000@ should always be an admin
                                                "admins": [0],                                      # Defined as user "0" or by full username.
                                                # These should be defined at the account level.
                                                # "instructors": [0],                               # User class+000@ should always be an instructor
                                                # "analysts": ["class+analyst@databricks.com"],     # "Special" config for DEWD/ADEWD v3
                                            })

account = AccountConfig.from_env(account_id_env_name=f"WORKSPACE_SETUP_ACCOUNT_ID",        # Loaded from the environment to make
                                 account_password_env_name=f"WORKSPACE_SETUP_PASSWORD",    # configuration easily swapped between systems
                                 account_username_env_name=f"WORKSPACE_SETUP_USERNAME",    # Refactor to use .databricks/profile-name/etc.
                                 first_workspace_number=first_workspace_number,
                                 total_workspaces=total_workspaces,
                                 region="us-west-2",
                                 uc_storage_config=uc_storage_config,
                                 workspace_config_template=workspace_config_template,
                                 ignored_workspaces=[  # Either the number or name (not domain) to skip
                                     # "classroom-005-9j1zg",
                                     # "classroom-007-t8yxo",
                                     # 9, 11, 24
                                 ])

workspace_setup = WorkspaceSetup(account)

# workspace_setup.remove_metastore(-1)
# workspace_setup.delete_workspace(-1)

FALSE = False
workspace_setup.create_workspaces(remove_users=FALSE,          # This really should always be False
                                  create_users=True,
                                  update_entitlements=True,
                                  create_groups=True,
                                  remove_metastore=FALSE,      # This really should always be False
                                  create_metastore=True,
                                  enable_features=True,
                                  run_workspace_setup=True,    # <<<
                                  uninstall_courseware=FALSE,  # This really should always be False
                                  install_courseware=True,
                                  workspace_numbers=range(first_workspace_number, first_workspace_number+total_workspaces))

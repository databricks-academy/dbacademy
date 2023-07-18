__validated_google_apis = False
try:
    # noinspection PyPackageRequirements
    import google
    # noinspection PyPackageRequirements
    import googleapiclient

except ModuleNotFoundError:
    if not __validated_google_apis:
        raise Exception("The following libraries are required, runtime-dependencies not bundled with the dbacademy library: google-api-python-client google-auth-httplib2 google-auth-oauthlib")

finally:
    __validated_google_apis = True

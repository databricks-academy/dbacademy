def create_client():
    from dbacademy.dbrest import DBAcademyRestClient
    import os
    import configparser

    for path in ('.databrickscfg', '~/.databrickscfg'):
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            continue
        config = configparser.ConfigParser()
        config.read(path)
        if 'DEFAULT' not in config:
            print('No Default')
            continue
        host = config['DEFAULT']['host'].rstrip("/")
        token = config['DEFAULT']['token']
        return DBAcademyRestClient(token, host)
    return DBAcademyRestClient()


databricks = create_client()

if __name__ == '__main__':
    from dbacademy.dbrest.tests.all import main
    main()

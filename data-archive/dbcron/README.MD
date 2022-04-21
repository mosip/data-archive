# DB Audit DB Archive cron job

This is kuberntes cronjob, that is configured to run everyday midnight UTC. Even though the db scripts provided here are for archive purpose, this can easly be modified to run any postgres related cronjob. 

## How to use 

1. Update ``username`` and ``passowrd`` secret under ``manifest/credentials.yaml ``

    ``` echo -n test-user | base64 ``` and ``` echo -n my-password | base64 ```
2. Update DB variables in ``manifests/variables.yaml`` i.e db host name and dbname. ``POSTGRES_HOST_AUTH_METHOD`` is required to start postgres container with out error.
3. Upadate your sql command under ``manifests/sql-script.yaml``.
4. Review cronjob maifest under ``manifests/cronjob.yaml``
5. apply maifest files `` kubectl apply -f manifests/ ``
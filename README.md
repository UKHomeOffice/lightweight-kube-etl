# SNS subscriber that runs Kubernetes job

Lightweight ETL in Kubernetes + AWS

This is designed to run in the cluster and use a service account.
It subscribes to a SQS queue which you should make your S3 bucket point at on writes.

If something enters the queue, this will pull the manifest.json of the bucket and see if the files in the manifest match that in the bucket by comparing the `SHA256`s.

If it's a match, then it will start your Kubernetes job.

It assumes you have defined your job in a cronjob, perhaps with `suspend:true`.

It will delete any currently running job made by the process, and then kick off a new one.

# Manifest file example:
```json
[
    {
        "FileName":  "file1a1.txt",
        "SHA256":  "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15"
    },
    {
        "FileName":  "testfile1a1.csv.gz",
        "SHA256":  "ba6119931c7010138eec96d9fb75701865908286"
    },
    {
        "FileName":  "testfile1b1.csv.gz",
        "SHA256":  "5a21580ea036c4863a3cd0fa810a937eef1bc5a8"
    }
]
```

# Cronjob example:
```yaml
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: myjob
spec:
  schedule: "* * * * *"
  suspend: true
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: myjob
              image: busybox
              command:
                - 'echo'
                - 'helloworld'
```

# Deployment of this example:
```yaml
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: kube-etl
spec:
  replicas: 3
  template:
    metadata:
      labels:
        name: kube-etl
    spec:
      containers:
        - name: api
          image: ukhomeoffice/lightweight-kube-etl
          env:
            - name: BUCKET
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: bucket_name
            - name: S3_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: access_key_id
            - name: S3_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: secret_access_key
            - name: QUEUE
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: sqs_queue
            - name: ROLE
              value: 'myjob'
            - name: CRONJOB
              value: 'myjob'

```
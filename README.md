![etl](https://user-images.githubusercontent.com/4499581/46379341-65cb0c80-c696-11e8-8e3b-354936e42645.jpg)

Lightweight ETL in Kubernetes + AWS

This is designed to run in the cluster and use a service account.

The script will keep looking inside an s3 bucket for timestamped folders in a folder named `pending`.

It will take the oldest folder and que it up for ingestion. When the manifest file is present then a Job will be started.

If it's a match, then it will start your Kubernetes job.

It assumes you have defined your job in a cronjob, perhaps with `suspend:true`.

It will delete any currently running job made by the process, and then kick off a new one.

# Manifest file example:

```json
[
  {
    "FileName": "file1a1.txt",
    "SHA256": "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15"
  },
  {
    "FileName": "testfile1a1.csv.gz",
    "SHA256": "ba6119931c7010138eec96d9fb75701865908286"
  },
  {
    "FileName": "testfile1b1.csv.gz",
    "SHA256": "5a21580ea036c4863a3cd0fa810a937eef1bc5a8"
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
                - "echo"
                - "helloworld"
```

To start it locally locally:

```
minikube start
kubectl create -f ./cronjob.example.yml
kubectl create job test-1 --from=cronjob/myjob
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
            - name: S3_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: access_key_id
            - name: BUCKET
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: bucket_name
            - name: S3_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: secret_access_key
            - name: QUEUE
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: queue.url
            - name: SQS_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: access.key
            - name: SQS_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: secret.key
            - name: REGION
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: region
            - name: CONTEXT
              valueFrom:
                secretKeyRef:
                  name: my-secret
                  key: context
            - name: ROLE
              value: "myjob"
            - name: CRONJOB
              value: "myjob"
```

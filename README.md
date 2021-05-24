# spark-tweeter

A demo sidecar container for use with [EMR on EKS pod templates](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/pod-templates.html).

![](tweets.png)

## Prerequisites

- A Twitter app and associated user credentials

## Usage

- Create a secret in your EKS cluster with your Twitter credentials

```shell
kubectl create secret generic -n emr-jobs twitter-creds \
  --from-literal=consumer_key=${CONSUMER_KEY} \
  --from-literal=consumer_secret=${CONSUMER_SECRET} \
  --from-literal=access_token=${ACCESS_TOKEN} \
  --from-literal=access_token_secret=${ACCESS_TOKEN_SECRET}
```

- Create a pod template file and upload to S3

_Note that the pod template file mounts two EMR volumes that contain logs and a heartbeat file._

```yaml
# tweetcar.yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: side-car-tweeter
    image: ghcr.io/dacort/spark-tweeter:latest
    env:
      - name: CONSUMER_KEY
        valueFrom:
          secretKeyRef:
            name: twitter-creds
            key: consumer_key
      - name: CONSUMER_SECRET
        valueFrom:
          secretKeyRef:
            name: twitter-creds
            key: consumer_secret
      - name: ACCESS_TOKEN
        valueFrom:
          secretKeyRef:
            name: twitter-creds
            key: access_token
      - name: ACCESS_TOKEN_SECRET
        valueFrom:
          secretKeyRef:
            name: twitter-creds
            key: access_token_secret
      - name: EMR_COMMS_MOUNT
        value: /var/log/fluentd
    resources: {}
    volumeMounts:
      - name: emr-container-application-log-dir
        mountPath: /var/log/spark/user
      - name: emr-container-communicate
        mountPath: /var/log/fluentd
```

```shell
aws s3 cp tweetcar.yaml s3://<BUCKET>/pod_templates/tweetcar.yaml
```

- Run your Spark job with this sidecar mounted on the Driver

```shell
aws emr-containers start-job-run \
    --virtual-cluster-id ${EMR_EKS_CLUSTER_ID} \
    --name dacort-tweeter \
    --execution-role-arn ${EMR_EKS_EXECUTION_ARN} \
    --release-label emr-5.33.0-latest \
    --job-driver '{
        "sparkSubmitJobDriver": {
            "entryPoint": "s3://'${S3_BUCKET}'/code/pyspark/windy_city.py",
            "sparkSubmitParameters": "--conf spark.executor.instances=20 --conf spark.executor.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1 --conf spark.kubernetes.driver.podTemplateFile=s3://'${S3_BUCKET}'/pod_templates/tweetcar.yaml"
        }
    }'
```
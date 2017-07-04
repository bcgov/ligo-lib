apiVersion: v1
kind: DeploymentConfig
metadata:
  name: {{ customconfig['name'] }}
  namespace: {{ customconfig['namespace'] }}
  selfLink: /oapi/v1/namespaces/{{ customconfig['namespace'] }}/deploymentconfigs/{{ customconfig['name'] }}
  labels:
    app: {{ customconfig['app_name'] }}
spec:
  strategy:
    type: {{spec_strategy_type}}
    rollingParams:
      updatePeriodSeconds: 1
      intervalSeconds: 1
      timeoutSeconds: 600
      maxUnavailable: 25%
      maxSurge: 25%
    resources: {}
    activeDeadlineSeconds: 21600
  triggers:
    - type: ConfigChange
    - type: ImageChange
      imageChangeParams:
        automatic: true
        containerNames:
          - {{ customconfig['container_name'] }}
        from:
          kind: ImageStreamTag
          namespace: openshift
          name: {{ customconfig['postgres_version_tag'] }}
  replicas: 1
  test: false
  selector:
    app: {{ customconfig['app_name'] }}
    deploymentconfig: {{ customconfig['deploymentconfig'] }}
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: {{ customconfig['app_name'] }}
        deploymentconfig: {{ customconfig['deploymentconfig'] }}
    spec:
      volumes:
        - name: {{ customconfig['volume_name'] }}
          emptyDir: {}
      containers:
        - name: {{ customconfig['container_name'] }}
          ports:
            - containerPort: {{ customconfig['container_port'] }}
              protocol: TCP
          env:
            - name: POSTGRESQL_USER
              value: {{ customconfig['postgres_user'] }}
            - name: POSTGRESQL_PASSWORD
              value: {{ customconfig['postgres_password'] }}
            - name: POSTGRESQL_DATABASE
              value: {{ customconfig['postgres_database'] }}
            - name: POSTGRESQL_HOST
              value: {{ customconfig['postgresql_host'] }}
          resources: {}
          volumeMounts:
            - name: {{ customconfig['volumes_name'] }}
              mountPath: /var/lib/pgsql/data
          terminationMessagePath: /dev/termination-log
          imagePullPolicy: Always
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      securityContext: {}

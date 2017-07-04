apiVersion: v1
kind: DeploymentConfig
metadata:
  name: {{ customconfig['name'] }}
  namespace: {{ customconfig['namespace'] }}
  selfLink: >-
    /oapi/v1/namespaces/{{ customconfig['namespace'] }}/deploymentconfigs/{{ customconfig['name'] }}
  labels:
    app: {{ customconfig['name'] }}
spec:
  strategy:
    type: Rolling
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
          - {{ customconfig['containername'] }}
        from:
          kind: ImageStreamTag
          namespace: linking-project
          name: {{ customconfig['imagename_and_tag'] }}
  replicas: 1
  test: false
  selector:
    app: {{ customconfig['name'] }}
    deploymentconfig: {{ customconfig['deploymentconfig'] }}
  template:
    metadata:
      creationTimestamp: null
      labels:
        app: {{ customconfig['name'] }}
        deploymentconfig: {{ customconfig['deploymentconfig'] }}
    spec:
      containers:
        - name: {{ customconfig['containername'] }}
          command:
            - {{ customconfig['enrty_point_command_part_1'] }}
            - {{ customconfig['enrty_point_command_part_2'] }}
          ports:
            - name: {{ port_name }}
              containerPort: {{ customconfig['containerport'] }}
              protocol: TCP
          env:
            - name: LINK_DB_USER
              value: {{ customconfig['db_user'] }}
            - name: LINK_DB_HOST
              value: {{ customconfig['db_host'] }}
            - name: LINK_DB_PORT
              value: {{ customconfig['db_port'] }}
            - name: LINK_DB_SERVICE
              value: {{ customconfig['db_service'] }}
            - name: LINK_DB_PASSWORD
              value: {{ customconfig['db_pssword'] }}
            - name: ALLOWED_HOST
              value: {{ customconfig['allowed_host'] }}
            - name: DJANGO_EMAIL_BACKEND
              value: django.core.mail.backends.filebased.EmailBackend
            - name: DJANGO_EMAIL_FILE_PATH
              value: /tmp/django-email-dev
            - name: CELERY_BROKER_URL
              value: {{ customconfig['celery_broker_url'] }}
          resources: {}
          terminationMessagePath: /dev/termination-log
          imagePullPolicy: IfNotPresent
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      securityContext:
        fsGroup: 999

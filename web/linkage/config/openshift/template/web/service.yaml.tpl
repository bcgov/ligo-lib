apiVersion: v1
kind: Service
metadata:
  name: {{ customconfig['name'] }}
  namespace: {{ customconfig['namespace'] }}
  selfLink: /api/v1/namespaces/{{ customconfig['namespace'] }}/services/{{ customconfig['name'] }}
  labels:
    app: {{ customconfig['app_name'] }}
    template: {{ customconfig['template_name'] }}
spec:
  ports:
    - name: {{ customconfig['port_name'] }}
      protocol: TCP
      port: {{ customconfig['port'] }}
      targetPort: {{ customconfig['target_port'] }}
  selector:
    deploymentconfig: {{ customconfig['deploymentconfig'] }}
  clusterIP: {{ customconfig['cluster_IP'] }}
  type: ClusterIP
  sessionAffinity: None
status:
  loadBalancer: {}
